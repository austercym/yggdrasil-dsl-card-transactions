package com.orwellg.yggdrasil.dsl.card.transactions.common.bolts;

import java.util.Arrays;
import java.util.Map;

public class GenericEventMappingBolt<TEventData> extends GenericEventProcessBolt<TEventData> {

    private static final long serialVersionUID = 1L;

    public GenericEventMappingBolt(Class<TEventData> eventDataType) {
        super(eventDataType);
    }

    @Override
    protected void process(Map<String, Object> values, TEventData eventData, String key, String processId) {
        Object processed = mapEvent(eventData, key, processId);
        values.put(Fields.EVENT_DATA, processed);
    }

    protected Object mapEvent(TEventData eventData, String key, String processId) {
        return eventData;
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA));
    }
}
