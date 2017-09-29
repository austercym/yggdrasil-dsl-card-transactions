package com.yggdrasil.dsl.card.transactions.topology.bolts.event;

import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;

public class SampleBolt extends BasicRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LogManager.getLogger(SampleBolt.class);

	@Override
	public void execute(Tuple input) {
	
		LOG.debug("Event received: {}. Starting the decode process.", input);
		
		try {
			Event event = null;
			if (input.getValues().get(4) != null && (input.getValues().get(4) instanceof ByteBuffer)) {
				event = RawMessageUtils.decode(Event.SCHEMA$, (ByteBuffer) input.getValues().get(4));
			} else if (input.getValues().get(4) instanceof String) {
				event = RawMessageUtils.decodeFromString(Event.SCHEMA$, (String) input.getValues().get(4));
			}
			String data = event.getEvent().getData();
			
			LOG.info("[Key: {}][ProcessId: ]: The event was decoded. Send the tuple to the next step in the Topology.", event.getEvent().getName());
		} catch (Exception e) {
			LOG.error("The received event {} can not be decoded. Message: {}", input, e.getMessage(), e);
			error(e, input);
		}
	}

	@Override
	public void declareFieldsDefinition() {
		addFielsDefinition(Arrays.asList(new String[] { "key", "processId", "eventData" }));
	}
}
