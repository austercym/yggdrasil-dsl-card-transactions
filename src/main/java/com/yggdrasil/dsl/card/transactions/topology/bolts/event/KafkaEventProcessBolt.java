package com.yggdrasil.dsl.card.transactions.topology.bolts.event;

import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.gps.Message;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KafkaEventProcessBolt
		extends com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaEventProcessBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final static Logger LOG = LogManager.getLogger(KafkaEventProcessBolt.class);

	@Override
	public void sendNextStep(Tuple input, Event event) {

		String key = event.getEvent().getKey();
		String processId = event.getProcessIdentifier().getUuid();

		LOG.info("[Key: {}][ProcessId: {}]: Received GPS message event", key, processId);

		// Get the JSON message with the data
		Message eventData = gson.fromJson(event.getEvent().getData(), Message.class);

		Map<String, Object> values = new HashMap<>();
		values.put("key", key);
		values.put("processId", processId);
		values.put("eventData", eventData);

		send(input, values);

		LOG.info("[Key: {}][ProcessId: {}]: GPS message event sent.", key, processId);
	}

	@Override
	public void declareFieldsDefinition() {
		addFielsDefinition(Arrays.asList("key", "processId", "eventData"));
	}
}
