package com.orwellg.yggdrasil.dsl.card.transactions.common.bolts;

import com.datastax.driver.core.Session;
import com.orwellg.umbrella.commons.repositories.scylla.CardTransactionRepository;
import com.orwellg.umbrella.commons.repositories.scylla.cards.TransactionMatchingRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.cards.CardTransactionRepositoryImpl;
import com.orwellg.umbrella.commons.repositories.scylla.impl.cards.TransactionMatchingRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.yggdrasil.card.transaction.commons.bolts.Fields;
import com.orwellg.yggdrasil.card.transaction.commons.config.ScyllaSessionFactory;
import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfig;
import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfigFactory;
import com.orwellg.yggdrasil.card.transaction.commons.model.TransactionInfo;
import com.orwellg.yggdrasil.card.transaction.commons.transactionmatching.TransactionMatcher;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadTransactionListBolt extends BasicRichBolt {

    private static final long serialVersionUID = 1L;

    private Logger LOG = LogManager.getLogger(LoadTransactionListBolt.class);

    private CardTransactionRepository transactionRepository;
    private TransactionMatchingRepository matchingRepository;
    private TransactionMatcher transactionMatcher;
    private String propertyFile;

    public LoadTransactionListBolt(String propertyFile) {
        this.propertyFile = propertyFile;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        String zookeeperHost = (String) stormConf.get(Constants.ZK_HOST_LIST_PROPERTY);
        TopologyConfig topologyConfig = TopologyConfigFactory.getTopologyConfig(propertyFile, zookeeperHost);
        initializeCardRepositories(topologyConfig);
        transactionMatcher = new TransactionMatcher(matchingRepository);
    }

    private void initializeCardRepositories(TopologyConfig topologyConfig) {
        Session session = ScyllaSessionFactory.getCardsSession(topologyConfig.getScyllaConfig());
        transactionRepository = new CardTransactionRepositoryImpl(session);
        matchingRepository = new TransactionMatchingRepositoryImpl(session);
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA, Fields.TRANSACTION_LIST));
    }

    @Override
    public void execute(Tuple input) {
        String logPrefix = null;

        try {
            String key = input.getStringByField(Fields.KEY);
            String processId = input.getStringByField(Fields.PROCESS_ID);
            TransactionInfo eventData = (TransactionInfo) input.getValueByField(Fields.EVENT_DATA);
            logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

            LOG.info("{}Matching transaction for: {}", logPrefix, eventData);
            eventData.setTransactionId(transactionMatcher.getMatchingTransactionId(eventData.getMessage()));

            List<CardTransaction> transactionList = getTransactionList(eventData.getTransactionId(), logPrefix);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_DATA, eventData);
            values.put(Fields.TRANSACTION_LIST, transactionList);

            send(input, values);

        } catch (Exception e) {
            LOG.error("{}Error retrieving transaction list. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    private List<CardTransaction> getTransactionList(String transactionId, String logPrefix) {
        LOG.info("{}Retrieving transaction list for TransactionId={} ...", logPrefix, transactionId);
        List<CardTransaction> transactionList = transactionRepository.getCardTransaction(transactionId);
        LOG.info("{}Retrieved transaction list for TransactionId={}: {}", logPrefix, transactionId, transactionList);
        return transactionList;
    }
}
