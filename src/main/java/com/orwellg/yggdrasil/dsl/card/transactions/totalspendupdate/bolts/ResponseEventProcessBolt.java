package com.orwellg.yggdrasil.dsl.card.transactions.totalspendupdate.bolts;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaEventProcessBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ResponseEventProcessBolt extends KafkaEventProcessBolt {

    private static final long serialVersionUID = 1L;

    private final static Logger LOG = LogManager.getLogger(ResponseEventProcessBolt.class);

    @Override
    public void sendNextStep(Tuple input, Event event) {
        String key = event.getEvent().getKey();
        String processId = event.getProcessIdentifier().getUuid();
        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        LOG.info("[Key: {}][ProcessId: {}]: Received message processed event", key, processId);

        try {
            // Get the JSON message with the data
            String data = event.getEvent().getData();
            MessageProcessed eventData = gson.fromJson(data, MessageProcessed.class);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_DATA, eventData);

            send(input, values);
        } catch (Exception e) {
            LOG.error("{} Error processing the message. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA));
    }
}