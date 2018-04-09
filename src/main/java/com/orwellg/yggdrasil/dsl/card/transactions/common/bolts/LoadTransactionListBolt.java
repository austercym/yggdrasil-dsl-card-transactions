package com.orwellg.yggdrasil.dsl.card.transactions.common.bolts;

import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.repositories.scylla.CardTransactionRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.CardTransactionRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.card.transaction.commons.model.TransactionInfo;
import com.orwellg.yggdrasil.dsl.card.transactions.config.TopologyConfigFactory;
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
    private String propertyFile;

    public LoadTransactionListBolt(String propertyFile) {
        this.propertyFile = propertyFile;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        initializeCardRepositories();
    }

    private void initializeCardRepositories() {
        ScyllaParams scyllaParams = TopologyConfigFactory.getTopologyConfig(propertyFile)
                .getScyllaConfig().getScyllaParams();
        String nodeList = scyllaParams.getNodeList();
        String keyspace = scyllaParams.getKeyspaceCardsDB();
        transactionRepository = new CardTransactionRepositoryImpl(nodeList, keyspace);
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

            List<CardTransaction> transactionList = getTransactionList(eventData.getProviderTransactionId(), logPrefix);

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

    private List<CardTransaction> getTransactionList(String gpsTransactionLink, String logPrefix) {
        LOG.info("{}Retrieving transaction list for ProviderTransactionId={} ...", logPrefix, gpsTransactionLink);
        List<CardTransaction> transactionList = transactionRepository.getCardTransaction(gpsTransactionLink);
        LOG.info("{}Retrieved transaction list for ProviderTransactionId={}: {}", logPrefix, gpsTransactionLink, transactionList);
        return transactionList;
    }
}
