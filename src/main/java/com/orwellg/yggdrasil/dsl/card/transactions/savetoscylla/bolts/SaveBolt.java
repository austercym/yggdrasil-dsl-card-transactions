package com.orwellg.yggdrasil.dsl.card.transactions.savetoscylla.bolts;

import com.datastax.driver.core.Session;
import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.repositories.scylla.CardTransactionRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.CardTransactionRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.umbrella.commons.utils.scylla.ScyllaManager;
import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfigFactory;
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
    private String propertyFile;

    public SaveBolt(String propertyFile) {
        this.propertyFile = propertyFile;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        setScyllaConnectionParameters();
    }

    private void setScyllaConnectionParameters() {
        ScyllaParams scyllaParams = TopologyConfigFactory.getTopologyConfig(propertyFile)
                .getScyllaConfig().getScyllaParams();
        ScyllaManager scyllaManager = ScyllaManager.getInstance(scyllaParams);
        Session session = scyllaManager.getSession(scyllaParams.getKeyspaceCardsDB());
        repository = new CardTransactionRepositoryImpl(session);
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_NAME, Fields.RESULT));
    }

    @Override
    public void execute(Tuple input) {
        String logPrefix = null;
        try {
            String key = input.getStringByField(Fields.KEY);
            String processId = input.getStringByField(Fields.PROCESS_ID);
            logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

            LOG.info("{}Saving card transaction to Scylla", logPrefix);
            CardTransaction transaction = (CardTransaction) input.getValueByField(Fields.EVENT_DATA);
            repository.addTransaction(transaction);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_NAME, CardTransactionEvents.SAVED_TO_SCYLLA.getEventName());
            values.put(Fields.RESULT, transaction);
            send(input, values);
        } catch (Exception e) {
            LOG.error("{}Error occurred when saving card transaction to Scylla. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }
}
