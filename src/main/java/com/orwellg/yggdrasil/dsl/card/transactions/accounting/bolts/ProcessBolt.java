package com.orwellg.yggdrasil.dsl.card.transactions.accounting.bolts;

import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaEventProcessBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.common.bolts.Fields;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class ProcessBolt extends KafkaEventProcessBolt {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(ProcessBolt.class);

    @Override
    public void sendNextStep(Tuple tuple, Event event) {

        String logPrefix = null;
        try {
            String eventKey = event.getEvent().getKey();
            String processId = event.getProcessIdentifier().getUuid();
            logPrefix = String.format("[Key: %s][ProcessId: %s] ", eventKey, processId);

            LOG.info("{}Received message", logPrefix);

            // Get the JSON message with the data
            GpsMessageProcessed eventData = gson.fromJson(event.getEvent().getData(), GpsMessageProcessed.class);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, eventKey);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_DATA, eventData);

            send(tuple, values);

            LOG.info("{}Message sent", logPrefix);
        } catch (Exception e) {
            LOG.error("{}Error processing message. Message: {}", logPrefix, e.getMessage(), e);
        }
    }
}
