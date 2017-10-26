package com.yggdrasil.dsl.card.transactions.topology.bolts.processors.presentment;

import com.orwellg.umbrella.avro.types.event.EntityIdentifierType;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.event.EventType;
import com.orwellg.umbrella.avro.types.event.ProcessIdentifierType;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.text.SimpleDateFormat;
import java.util.*;

public class PresentmentOfflineMockBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(PresentmentOfflineMockBolt.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "message"));
    }

    @Override
    public void execute(Tuple tuple) {

        //todo:
        //1. check if account exists
        //2. check if sufficient funds
        //3. calculate fees
        //4. send complete message via kafka
        //2,3 can be in parallel

        try {

            List<Object> inputValues = tuple.getValues();
            String key = (String) inputValues.get(0);
            String originalProcessId = (String)inputValues.get(1);
            Message eventData = (Message) inputValues.get(2);

            GpsMessageProcessed message = generateMessageProcessed(eventData);
            Event presentmentEvent = generteEvent(this.getClass().getName()
                    , CardTransactionEvents.RESPONSE_MESSAGE.getEventName()
                    ,key
                    ,message);

            Map<String, Object> values = new HashMap<>();
            values.put("key", key);
            values.put("processId", originalProcessId);
            values.put("eventData", tuple);
            values.put("message", RawMessageUtils.encodeToString(Event.SCHEMA$, presentmentEvent));

            send(tuple, values);

        } catch (Exception e) {
            //todo: error
            LOG.error("The received event {} can not be decoded. Message: {}", tuple, e.getMessage(), e);
            error(e, tuple);
        }
    }

    private GpsMessageProcessed generateMessageProcessed(Message eventData){

        LOG.debug("Generating gpsMessageProcessed message");

        GpsMessageProcessed gpsMessageProcessed = new GpsMessageProcessed();
        gpsMessageProcessed.setGpsTransactionLink(eventData.getTransLink());
        gpsMessageProcessed.setGpsTransactionId(eventData.getTXnID());
        gpsMessageProcessed.setDebitCardId(1l);
        gpsMessageProcessed.setTransactionTimestamp(new Date().toString());      //todo: is this a correct field?
        gpsMessageProcessed.setInternalAccountId(1l);
        gpsMessageProcessed.setOriginalWirecardAmount(-eventData.getSettleAmt()); //in normal flow the settlement amount comes to us as a negative value
        gpsMessageProcessed.setWirecardAmount(-eventData.getSettleAmt());
        gpsMessageProcessed.setWirecardCurrency(eventData.getSettleCcy());
        gpsMessageProcessed.setAppliedWirecardAmount(0d);
        gpsMessageProcessed.setBlockedClientAmount(eventData.getSettleAmt());
        gpsMessageProcessed.setBlockedClientCurrency(eventData.getSettleCcy());
        gpsMessageProcessed.setAppliedBlockedClientAmount(0d);
        gpsMessageProcessed.setOriginalBlockedClientAmount(eventData.getSettleAmt());

        LOG.debug("Message generated. Parameters: {}", gpsMessageProcessed);

        return gpsMessageProcessed;
    }

    private Event generteEvent(String eventName, String source, String parentKey, Object eventData){

        LOG.debug("Generating event with presentment data");

        String uuid = UUID.randomUUID().toString();

        // Create the event type
        EventType eventType = new EventType();
        eventType.setName(eventName);
        eventType.setVersion(Constants.getDefaultEventVersion()); //??
        eventType.setParentKey(Constants.EMPTY);
        eventType.setKey("EVENT-" + uuid); //todo: should this be the key?
        eventType.setSource(source);
        eventType.setParentKey(parentKey);
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

        LOG.debug("Eevent with presentment data generated correctly. Parameters: {}", eventData);

        return event;
    }
}
