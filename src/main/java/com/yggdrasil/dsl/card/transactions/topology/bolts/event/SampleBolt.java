package com.yggdrasil.dsl.card.transactions.topology.bolts.event;

import com.orwellg.umbrella.avro.types.event.EntityIdentifierType;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.event.EventType;
import com.orwellg.umbrella.avro.types.event.ProcessIdentifierType;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.avro.types.gps.ResponseMsg;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.text.SimpleDateFormat;
import java.util.*;

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
			Message event = (Message) input.getValues().get(2);

			LOG.info(
			        "Token: {}, TxnType: {}, Amount: {} {}",
                    event.getToken(), event.getTxnType(), event.getTxnAmt(), event.getTxnCCy());

            ResponseMsg response = new ResponseMsg();
            response.setAcknowledgement("1");
            response.setResponsestatus("00");
            response.setAvlBalance(19.09);
            response.setCurBalance(20.15);

            Event responseEvent = generateEvent(this.getClass().getName(), CardTransactionEvents.RESPONSE_MESSAGE.getEventName(), response);

            Map<String, Object> values = new HashMap<>();
            values.put("key", input.getStringByField("key"));
            values.put("message", RawMessageUtils.encodeToString(Event.SCHEMA$, responseEvent));
            values.put("topic", "com.orwellg.gps.response");

            send(input, values);

		} catch (Exception e) {
			LOG.error("The received event {} can not be decoded. Message: {}", input, e.getMessage(), e);
			error(e, input);
		}
	}
    private Event generateEvent(String source, String eventName, Object eventData) {

        LOG.debug("Generating event with response data.");

        String uuid = UUID.randomUUID().toString();

        // Create the event type
        EventType eventType = new EventType();
        eventType.setName(eventName);
        eventType.setVersion(Constants.getDefaultEventVersion());
        eventType.setParentKey(Constants.EMPTY);
        eventType.setKey("EVENT-" + uuid);
        eventType.setSource(source);
        SimpleDateFormat format = new SimpleDateFormat(Constants.getDefaultEventTimestampFormat());
        eventType.setTimestamp(format.format(new Date()));
        eventType.setData(eventData.toString());

        ProcessIdentifierType processIdentifier = new ProcessIdentifierType();
        processIdentifier.setUuid("PROCESS-" + uuid);

        EntityIdentifierType entityIdentifier = new EntityIdentifierType();
        entityIdentifier.setEntity(Constants.IPAGOO_ENTITY);
        entityIdentifier.setBrand(Constants.IPAGOO_BRAND);

        // Create the correspondent event
        Event event = new Event();
        event.setEvent(eventType);
        event.setProcessIdentifier(processIdentifier);
        event.setEntityIdentifier(entityIdentifier);

        LOG.debug("Response event generated correctly. Parameters: {}", eventData);

        return event;
    }

	@Override
	public void declareFieldsDefinition() {
		addFielsDefinition(Arrays.asList("key", "message", "topic"));
	}
}
