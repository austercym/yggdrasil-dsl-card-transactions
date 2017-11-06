package com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.processors;

import com.orwellg.umbrella.commons.repositories.FeeHistoryRepository;
import com.orwellg.umbrella.commons.repositories.scylla.FeeHistoryReposotoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.generics.scylla.ScyllaRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.FeeSchema;
import com.orwellg.yggdrasil.dsl.card.transactions.GpsMessage;
import com.orwellg.yggdrasil.dsl.card.transactions.topology.CardPresentmentDSLTopology;
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

public class FeeSchemaBolt extends ScyllaRichBolt<List<FeeSchema>, GpsMessage> {

    private FeeHistoryRepository repository;
    private static final Logger LOG = LogManager.getLogger(FeeSchemaBolt.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "retrieveValue", "gpsMessage"));
        addFielsDefinition(CardPresentmentDSLTopology.ERROR_STREAM, Arrays.asList("key", "processId", "eventData"));
    }

    @Override
    protected void setScyllaConnectionParameters() {
        setScyllaNodes(ComponentFactory.getConfigurationParams().getScyllaConfig().getScyllaParams().getNodeList());
        setScyllaKeyspace(ComponentFactory.getConfigurationParams().getScyllaConfig().getScyllaParams().getKeyspace());
    }

    @Override
    protected List<FeeSchema> retrieve(GpsMessage presentment) {
        List<FeeSchema> cardSettings =
                repository.getFeeHistory(presentment.getDebitCardId(), presentment.getTransactionTimestamp(),presentment.getFeeTransactionType().toString());
        return cardSettings;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        repository = new FeeHistoryReposotoryImpl(getScyllaNodes(), getScyllaKeyspace());
    }


    @Override
    public void execute(Tuple input) {

        try {

            Map<String, Object> values = new HashMap<>();
            values.put("key", input.getStringByField("key"));
            values.put("processId", input.getStringByField("processId"));
            values.put("eventData", input.getValueByField("eventData"));
            values.put("gpsMessage", input.getValueByField("gpsMessage"));
            values.put("retrieveValue", retrieve((GpsMessage) input.getValueByField("gpsMessage")));

            send(input, values);
        } catch (Exception e) {

            LOG.error("Error retrieving fee schema history information. Message: {}", input, e.getMessage(), e);

            Map<String, Object> values = new HashMap<>();
            values.put("key", input.getValueByField("key"));
            values.put("processId", input.getValueByField("processId"));
            values.put("eventData", input.getValueByField("eventData"));

            send(CardPresentmentDSLTopology.ERROR_STREAM, input, values);
            LOG.info("Error when processing GpsMessage - error send to corresponded kafka topic. Tuple: {}, Message: {}, Error: {}", input, e.getMessage(), e);
        }
    }
}
