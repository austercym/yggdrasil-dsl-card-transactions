package com.yggdrasil.dsl.card.transactions.topology.bolts.event;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.gps.Message;

public class KafkaEventProcessBolt
		extends com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaEventProcessBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private final static Logger LOG = LogManager.getLogger(KafkaEventProcessBolt.class);

	@Override
	public void sendNextStep(Tuple input, Event event) {

		String key = event.getEvent().getKey().toString();
		String processId = event.getProcessIdentifier().getUuid().toString();

		LOG.info("[Key: {}][ProcessId: {}]: Received GPS message event", key, processId);

		// Get the JSON message with the datas
		Message eventData = gson.fromJson(event.getEvent().getData().toString(), Message.class);

		Map<String, Object> values = new HashMap<>();
		values.put("key", key);
		values.put("processId", processId);
		values.put("eventData", eventData);

		send(input, values);

		LOG.info("[Key: {}][ProcessId: {}]: GPS message event sent.", key, processId);
	}
}
