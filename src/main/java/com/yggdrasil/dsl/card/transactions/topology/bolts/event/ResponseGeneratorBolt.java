package com.yggdrasil.dsl.card.transactions.topology.bolts.event;

import com.orwellg.umbrella.avro.types.event.EntityIdentifierType;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.event.EventType;
import com.orwellg.umbrella.avro.types.event.ProcessIdentifierType;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.avro.types.gps.ResponseMsg;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.ResponseCode;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.yggdrasil.dsl.card.transactions.services.ValidationResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.text.SimpleDateFormat;
import java.util.*;

public class ResponseGeneratorBolt extends BasicRichBolt {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(ResponseGeneratorBolt.class);

    @Override
    public void execute(Tuple input) {

        LOG.debug("Event received: {}. Starting the decode process.", input);

        try {
            Message event = (Message) input.getValueByField("eventData");
            String parentKey = (String) input.getValueByField("key");
            ValidationResult statusValidationResult = (ValidationResult) input.getValueByField("statusValidationResult");
            ValidationResult transactionTypeValidationResult = (ValidationResult) input.getValueByField("transactionTypeValidationResult");
            ValidationResult merchantValidationResult = (ValidationResult) input.getValueByField("merchantValidationResult");

            String logPrefix = String.format(
                    "[TransLink: %s, TxnId: %s, DebitCardId: %s, Token: %s, Amount: %s %s] ",
                    event.getTransLink(), event.getTXnID(),
                    event.getToken(), event.getCustRef(), event.getTxnAmt(), event.getTxnCCy());
            LOG.debug("{}Generating response for authorisation message...", logPrefix);

            ResponseCode responseCode = ResponseCode.DO_NOT_HONOUR;
            ResponseMsg response = new ResponseMsg();
            if (statusValidationResult.getIsValid()
                    && transactionTypeValidationResult.getIsValid()
                    && merchantValidationResult.getIsValid()) {
                responseCode = ResponseCode.ALL_GOOD;
                response.setAvlBalance(19.09);
                response.setCurBalance(20.15);
            }

            response.setAcknowledgement("1");
            response.setResponsestatus(responseCode.getCode());

            GpsMessageProcessed processedMessage = generateMessageProcessed(event, response);

            Event responseEvent = generateEvent(
                    this.getClass().getName(), CardTransactionEvents.RESPONSE_MESSAGE.getEventName(), processedMessage,
                    parentKey);

            Map<String, Object> values = new HashMap<>();
            values.put("key", input.getStringByField("key"));
            values.put("message", RawMessageUtils.encodeToString(Event.SCHEMA$, responseEvent));
            // TODO: get response topic from input event
            values.put("topic", "com.orwellg.gps.response");

            send(input, values);

            LOG.info(
                    "{}Response code for authorisation message: {} ({}))",
                    logPrefix, response.getResponsestatus(), responseCode);

        } catch (Exception e) {
            LOG.error("The received event {} can not be decoded. Message: {}", input, e.getMessage(), e);
            error(e, input);
        }
    }

    private GpsMessageProcessed generateMessageProcessed(Message authorisation, ResponseMsg response){

        LOG.debug("Generating gpsMessageProcessed message");
        GpsMessageProcessed gpsMessageProcessed = new GpsMessageProcessed();
        gpsMessageProcessed.setGpsTransactionLink(authorisation.getTransLink());
        gpsMessageProcessed.setGpsTransactionId(authorisation.getTXnID());
        gpsMessageProcessed.setDebitCardId(Long.parseLong(authorisation.getCustRef()));
        //gpsMessageProcessed.setTransactionTimestamp(authorisation.getTxnGPSDate());      //todo: is this a correct field?
        gpsMessageProcessed.setEhiResponse(response);

        LOG.debug("Message generated. Parameters: {}", gpsMessageProcessed);

        return gpsMessageProcessed;
    }

    private Event generateEvent(String source, String eventName, Object eventData, String parentKey) {

        LOG.debug("Generating event with response data.");

        String uuid = UUID.randomUUID().toString();

        // Create the event type
        EventType eventType = new EventType();
        eventType.setName(eventName);
        eventType.setVersion(Constants.getDefaultEventVersion());
        eventType.setParentKey(Constants.EMPTY);
        eventType.setKey("EVENT-" + uuid);
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

        LOG.debug("Response event generated correctly. Parameters: {}", eventData);

        return event;
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "message", "topic"));
    }
}
