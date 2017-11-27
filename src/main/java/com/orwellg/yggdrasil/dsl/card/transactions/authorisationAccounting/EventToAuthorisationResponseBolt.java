package com.orwellg.yggdrasil.dsl.card.transactions.authorisationAccounting;

import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaEventProcessBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class EventToAuthorisationResponseBolt extends KafkaEventProcessBolt {

	private static final long serialVersionUID = 1L;

	private final static Logger LOG = LogManager.getLogger(EventToAuthorisationResponseBolt.class);

	@Override
	public void sendNextStep(Tuple input, Event event) {

		String key = event.getEvent().getKey();
		String processId = event.getProcessIdentifier().getUuid();

		LOG.info("[Key: {}][ProcessId: {}]: Received GPS message event", key, processId);

		// Get the JSON message with the data
		GpsMessageProcessed eventData = gson.fromJson(event.getEvent().getData(), GpsMessageProcessed.class);

		Map<String, Object> values = new HashMap<>();
		values.put(Fields.KEY, key);
		values.put(Fields.PROCESS_ID, processId);
		values.put(Fields.EVENT_DATA, eventData);

		send(input, values);

		LOG.info("[Key: {}][ProcessId: {}]: GPS message event sent.", key, processId);
	}

	@Override
	public void declareFieldsDefinition() {
		addFielsDefinition(Arrays.asList(Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA));
	}
}
