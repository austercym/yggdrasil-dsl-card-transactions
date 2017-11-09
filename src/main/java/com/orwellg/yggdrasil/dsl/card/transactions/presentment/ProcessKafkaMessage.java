package com.orwellg.yggdrasil.dsl.card.transactions.presentment;

import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.event.KafkaEventProcessBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ProcessKafkaMessage extends com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaEventProcessBolt {

    /**
     *
     */
    private static final long serialVersionUID = 1L;

    private final static Logger LOG = LogManager.getLogger(KafkaEventProcessBolt.class);

    private PresentmentMessageMapper mapper;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        mapper = new PresentmentMessageMapper();
    }

    @Override
    public void sendNextStep(Tuple input, Event event) {

        try {
            String key = event.getEvent().getKey();
            String processId = event.getProcessIdentifier().getUuid();

            LOG.info("[Key: {}][ProcessId: {}]: Received GPS message event", key, processId);

            // Get the JSON message with the data
            Message eventData = gson.fromJson(event.getEvent().getData(), Message.class);
            PresentmentMessage message = mapper.map(eventData);

            Map<String, Object> values = new HashMap<>();
            values.put("key", key);
            values.put("processId", processId);
            values.put("eventData", eventData);
            values.put("gpsMessage", message);

            send(input, values);

            LOG.info("[Key: {}][ProcessId: {}]: GPS message event sent.", key, processId);
        }catch (Exception e){
            //todo: exception stream
        }
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "gpsMessage"));
        addFielsDefinition(CardPresentmentDSLTopology.ERROR_STREAM, Arrays.asList("key", "processId", "eventData", "exceptionMessage", "exceptionStackTrace"));
    }

}
