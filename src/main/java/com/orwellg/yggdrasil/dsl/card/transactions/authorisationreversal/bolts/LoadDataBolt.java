package com.orwellg.yggdrasil.dsl.card.transactions.authorisationreversal.bolts;

import com.orwellg.umbrella.commons.repositories.scylla.CardTransactionRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.CardTransactionRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.JoinFutureBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.dsl.card.transactions.config.ScyllaParams;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadDataBolt extends JoinFutureBolt<TransactionInfo> {

    private static final long serialVersionUID = 1L;

    private Logger LOG = LogManager.getLogger(LoadDataBolt.class);

    private CardTransactionRepository transactionRepository;

    public LoadDataBolt(String joinId) {
        super(joinId);
    }

    @Override
    public String getEventSuccessStream() {
        return KafkaSpout.EVENT_SUCCESS_STREAM;
    }

    @Override
    public String getEventErrorStream() {
        return KafkaSpout.EVENT_ERROR_STREAM;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        initializeCardRepositories();
    }

    private void initializeCardRepositories() {
        ScyllaParams scyllaParams = ComponentFactory.getConfigurationParams().getCardsScyllaParams();
        String nodeList = scyllaParams.getNodeList();
        String keyspace = scyllaParams.getKeyspace();
        transactionRepository = new CardTransactionRepositoryImpl(nodeList, keyspace);
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA, Fields.TRANSACTION_LIST));
    }

    @Override
    protected void join(Tuple input, String key, String processId, TransactionInfo eventData) {
        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        try {
            LOG.info("{}Retrieving transaction list for GpsTransactionLink {}", logPrefix, eventData.getProviderTransactionId());
            List<CardTransaction> transactionList = transactionRepository.getCardTransaction(eventData.getProviderTransactionId());

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

}
