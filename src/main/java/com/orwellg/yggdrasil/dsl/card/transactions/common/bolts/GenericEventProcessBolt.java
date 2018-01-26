package com.orwellg.yggdrasil.dsl.card.transactions.common.bolts;

import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaEventProcessBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class GenericEventProcessBolt<TEventData> extends KafkaEventProcessBolt {

    private static final long serialVersionUID = 1L;

    private final static Logger LOG = LogManager.getLogger(GenericEventProcessBolt.class);
    private Type eventDataType;

    public GenericEventProcessBolt(Class<TEventData> eventDataType) {
        this.eventDataType = eventDataType;
    }

    @Override
    public void sendNextStep(Tuple input, Event event) {

        String key = event.getEvent().getKey();
        String processId = event.getProcessIdentifier().getUuid();

        LOG.debug("[Key: {}][ProcessId: {}]: Processing event", key, processId);

        // Get the JSON message with the data
        TEventData eventData = gson.fromJson(event.getEvent().getData(), eventDataType);
        Object processed = process(eventData, key, processId);

        Map<String, Object> values = new HashMap<>();
        values.put(Fields.KEY, key);
        values.put(Fields.PROCESS_ID, processId);
        values.put(Fields.EVENT_DATA, processed);

        send(input, values);

        LOG.debug("[Key: {}][ProcessId: {}]: Event processed.", key, processId);
    }

    protected Object process(TEventData eventData, String key, String processId) {
        return eventData;
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA));
    }
}
