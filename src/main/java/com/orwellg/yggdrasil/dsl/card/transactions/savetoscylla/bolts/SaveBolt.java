package com.orwellg.yggdrasil.dsl.card.transactions.savetoscylla.bolts;

import com.orwellg.umbrella.commons.repositories.scylla.CardTransactionRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.CardTransactionRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.dsl.card.transactions.config.ScyllaParams;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SaveBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(SaveBolt.class);

    private CardTransactionRepository repository;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        setScyllaConnectionParameters();
    }

    private void setScyllaConnectionParameters() {
        ScyllaParams scyllaParams = ComponentFactory.getConfigurationParams().getCardsScyllaParams();
        String nodeList = scyllaParams.getNodeList();
        String keyspace = scyllaParams.getKeyspace();
        repository = new CardTransactionRepositoryImpl(nodeList, keyspace);
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.TRANSACTION));
    }

    @Override
    public void execute(Tuple input) {
        String logPrefix = null;
        try {
            String key = input.getStringByField(Fields.KEY);
            String processId = input.getStringByField(Fields.PROCESS_ID);
            logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

            LOG.info("{}Saving card transaction to Scylla", logPrefix);
            CardTransaction transaction = (CardTransaction) input.getValueByField(Fields.TRANSACTION);
            repository.addTransaction(transaction);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.TRANSACTION, transaction);
            send(input, values);
        } catch (Exception e) {
            LOG.error("{}Error occurred when saving card transaction to Scylla. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }
}
