package com.orwellg.yggdrasil.dsl.card.transactions.presentment;

import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.repositories.scylla.FeeHistoryRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.FeeHistoryReposotoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.generics.scylla.ScyllaRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.FeeSchema;
import com.orwellg.yggdrasil.dsl.card.transactions.model.PresentmentMessage;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetFeeSchema extends ScyllaRichBolt<List<FeeSchema>, PresentmentMessage> {

    private FeeHistoryRepository repository;
    private static final Logger LOG = LogManager.getLogger(GetFeeSchema.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "retrieveValue", "gpsMessage"));
    }

    @Override
    protected void setScyllaConnectionParameters() {
        setScyllaNodes(ComponentFactory.getConfigurationParams().getCardsScyllaParams().getNodeList());
        setScyllaKeyspace(ComponentFactory.getConfigurationParams().getCardsScyllaParams().getKeyspace());
    }

    @Override
    protected List<FeeSchema> retrieve(PresentmentMessage presentment) {
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

            String key = input.getStringByField("key");
            String processId = input.getStringByField("processId");
            Message eventData = (Message) input.getValueByField("eventData");
            PresentmentMessage presentmentMessage = (PresentmentMessage) input.getValueByField("gpsMessage");

            LOG.debug("Key: {} | ProcessId: {} | Retrieving Fee Schema from db.", key, processId);

            Map<String, Object> values = new HashMap<>();
            values.put("key", key);
            values.put("processId", processId);
            values.put("eventData", eventData);
            values.put("gpsMessage", presentmentMessage);
            values.put("retrieveValue", retrieve(presentmentMessage));
            send(input, values);

        } catch (Exception e) {

            LOG.error("Error retrieving Fee Schema from db. Tuple: {}, Message: {}, Error: {}", input, e.getMessage(), e);
            error(e, input);

        }
    }
}
