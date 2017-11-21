package com.orwellg.yggdrasil.dsl.card.transactions.authorisation;

import com.orwellg.umbrella.avro.types.event.EntityIdentifierType;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.event.EventType;
import com.orwellg.umbrella.avro.types.event.ProcessIdentifierType;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.avro.types.gps.ResponseMsg;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.accounting.AccountTransactionLog;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.ResponseCode;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.yggdrasil.dsl.card.transactions.model.AuthorisationMessage;
import com.orwellg.yggdrasil.dsl.card.transactions.services.ValidationResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

public class ResponseGeneratorBolt extends BasicRichBolt {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(ResponseGeneratorBolt.class);

    @Override
    public void execute(Tuple input) {

        LOG.debug("Event received: {}. Starting the decode process.", input);

        try {
            AuthorisationMessage event = (AuthorisationMessage) input.getValueByField(Fields.EVENT_DATA);
            String parentKey = (String) input.getValueByField(Fields.KEY);
            AccountTransactionLog accountTransactionLog =
                    (AccountTransactionLog) input.getValueByField(Fields.TRANSACTION_LOG);
            ValidationResult statusValidationResult =
                    (ValidationResult) input.getValueByField(Fields.STATUS_VALIDATION_RESULT);
            ValidationResult transactionTypeValidationResult =
                    (ValidationResult) input.getValueByField(Fields.TRANSACTION_TYPE_VALIDATION_RESULT);
            ValidationResult merchantValidationResult =
                    (ValidationResult) input.getValueByField(Fields.MERCHANT_VALIDATION_RESULT);
            ValidationResult velocityLimitsValidationResult =
                    (ValidationResult) input.getValueByField(Fields.VELOCITY_LIMITS_VALIDATION_RESULT);
            ValidationResult balanceValidationResult =
                    (ValidationResult) input.getValueByField(Fields.BALANCE_VALIDATION_RESULT);

            String logPrefix = String.format(
                    "[TransLink: %s, TxnId: %s, DebitCardId: %s, Token: %s, Amount: %s %s] ",
                    event.getGpsTransactionLink(), event.getGpsTransactionId(),
                    event.getDebitCardId(), event.getCardToken(),
                    event.getTransactionAmount(), event.getTransactionCurrency());
            LOG.debug("{}Generating response for authorisation message...", logPrefix);

            ResponseCode responseCode = ResponseCode.DO_NOT_HONOUR;
            ResponseMsg response = new ResponseMsg();
            if (!balanceValidationResult.getIsValid()) {
                responseCode = ResponseCode.INSUFFICIENT_FUNDS;
            } else if (!velocityLimitsValidationResult.getIsValid()) {
                responseCode = ResponseCode.EXCEEDS_WITHDRAWAL_AMOUNT_LIMIT;
            } else if (statusValidationResult.getIsValid()
                    && transactionTypeValidationResult.getIsValid()
                    && merchantValidationResult.getIsValid()) {
                responseCode = ResponseCode.ALL_GOOD;
                BigDecimal availableBalance =
                        accountTransactionLog.getActualBalance().subtract(event.getSettlementAmount());
                response.setAvlBalance(availableBalance.doubleValue());
                response.setCurBalance(accountTransactionLog.getLedgerBalance().doubleValue());
            }

            response.setAcknowledgement("1");
            response.setResponsestatus(responseCode.getCode());

            GpsMessageProcessed processedMessage = generateMessageProcessed(event, response);

            Event responseEvent = generateEvent(
                    this.getClass().getName(), CardTransactionEvents.RESPONSE_MESSAGE.getEventName(), processedMessage,
                    parentKey);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, input.getStringByField(Fields.KEY));
            values.put(Fields.MESSAGE, RawMessageUtils.encodeToString(Event.SCHEMA$, responseEvent));
            // TODO: get response topic from input event
            values.put(Fields.TOPIC, "com.orwellg.gps.authorisation.response.1");

            send(input, values);

            LOG.info(
                    "{}Response code for authorisation message: {} ({}))",
                    logPrefix, response.getResponsestatus(), responseCode);

        } catch (Exception e) {
            LOG.error("The received event {} can not be decoded. Message: {}", input, e.getMessage(), e);
            error(e, input);
        }
    }

    private GpsMessageProcessed generateMessageProcessed(AuthorisationMessage authorisation, ResponseMsg response) {

        LOG.debug("Generating gpsMessageProcessed message");
        GpsMessageProcessed gpsMessageProcessed = new GpsMessageProcessed();
        gpsMessageProcessed.setGpsMessageType(authorisation.getOriginalMessage().getTxnType());
        gpsMessageProcessed.setGpsTransactionLink(authorisation.getGpsTransactionLink());
        gpsMessageProcessed.setGpsTransactionId(authorisation.getGpsTransactionId());
        gpsMessageProcessed.setDebitCardId(authorisation.getDebitCardId());
        gpsMessageProcessed.setEhiResponse(response);
        gpsMessageProcessed.setSpendGroup(authorisation.getSpendGroup());
        gpsMessageProcessed.setTransactionTimestamp(new Date().getTime());

        // TODO: Where should be set amount to increase spend amount?

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
        addFielsDefinition(Arrays.asList(Fields.KEY, Fields.PROCESS_ID, "message", "topic"));
    }
}
