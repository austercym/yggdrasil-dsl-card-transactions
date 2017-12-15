package com.orwellg.yggdrasil.dsl.card.transactions.authorisation;

import com.orwellg.umbrella.avro.types.event.EntityIdentifierType;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.event.EventType;
import com.orwellg.umbrella.avro.types.event.ProcessIdentifierType;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.avro.types.gps.ResponseMsg;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.accounting.AccountTransactionLog;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.ResponseCode;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
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
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ResponseGeneratorBolt extends BasicRichBolt {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(ResponseGeneratorBolt.class);

    @Override
    public void execute(Tuple input) {

        LOG.debug("Preparing response for input={}", input);

        try {
            AuthorisationMessage event = (AuthorisationMessage) input.getValueByField(Fields.EVENT_DATA);
            String parentKey = (String) input.getValueByField(Fields.KEY);
            CardSettings settings = (CardSettings) input.getValueByField(Fields.CARD_SETTINGS);
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
            String responseKey = input.getStringByField(Fields.RESPONSE_KEY);

            String logPrefix = String.format(
                    "[Key: %s, TransLink: %s, TxnId: %s, DebitCardId: %s, Token: %s, Amount: %s %s] ",
                    parentKey, event.getGpsTransactionLink(), event.getGpsTransactionId(),
                    event.getDebitCardId(), event.getCardToken(),
                    event.getTransactionAmount(), event.getTransactionCurrency());
            LOG.debug("{}Generating response for authorisation message...", logPrefix);

            ResponseCode responseCode = ResponseCode.DO_NOT_HONOUR;
            ResponseMsg response = new ResponseMsg();
            BigDecimal earmarkAmount = BigDecimal.ZERO;
            String earmarkCurrency = null;
            if (!balanceValidationResult.getIsValid()) {
                responseCode = ResponseCode.INSUFFICIENT_FUNDS;
            } else if (!velocityLimitsValidationResult.getIsValid()) {
                responseCode = ResponseCode.EXCEEDS_WITHDRAWAL_AMOUNT_LIMIT;
            } else if (statusValidationResult.getIsValid()
                    && transactionTypeValidationResult.getIsValid()
                    && merchantValidationResult.getIsValid()) {
                responseCode = ResponseCode.ALL_GOOD;
                earmarkAmount = event.getSettlementAmount();
                earmarkCurrency = event.getSettlementCurrency();

                if (accountTransactionLog != null) {
                    double availableBalance =
                            accountTransactionLog.getActualBalance() == null
                            ? event.getSettlementAmount().negate().doubleValue()
                            : accountTransactionLog.getActualBalance().subtract(event.getSettlementAmount()).doubleValue();
                    double currentBalance = accountTransactionLog.getLedgerBalance() == null
                            ? 0
                            : accountTransactionLog.getLedgerBalance().doubleValue();

                    response.setAvlBalance(availableBalance);
                    response.setCurBalance(currentBalance);
                }
            }

            response.setAcknowledgement("1");
            response.setResponsestatus(responseCode.getCode());

            GpsMessageProcessed processedMessage = generateMessageProcessed(event, response, settings, earmarkAmount, earmarkCurrency);

            Event responseEvent = generateEvent(
                    this.getClass().getName(), CardTransactionEvents.RESPONSE_MESSAGE.getEventName(), processedMessage,
                    responseKey, parentKey);

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
            LOG.error("Response generation failed - input={}, message={}", input, e.getMessage(), e);
            error(e, input);
        }
    }

    private GpsMessageProcessed generateMessageProcessed(AuthorisationMessage authorisation, ResponseMsg response, CardSettings settings, BigDecimal earmarkAmount, String earmarkCurrency) {

        LOG.debug("Generating gpsMessageProcessed message");
        GpsMessageProcessed gpsMessageProcessed = new GpsMessageProcessed();
        gpsMessageProcessed.setGpsMessageType(authorisation.getOriginalMessage().getTxnType());
        gpsMessageProcessed.setGpsTransactionLink(authorisation.getGpsTransactionLink());
        gpsMessageProcessed.setGpsTransactionId(authorisation.getGpsTransactionId());
        gpsMessageProcessed.setDebitCardId(authorisation.getDebitCardId());
        gpsMessageProcessed.setWirecardAmount(DecimalTypeUtils.toDecimal(authorisation.getSettlementAmount()));
        gpsMessageProcessed.setWirecardCurrency(authorisation.getSettlementCurrency());
        gpsMessageProcessed.setFeesAmount(DecimalTypeUtils.toDecimal(0));
        gpsMessageProcessed.setEhiResponse(response);
        gpsMessageProcessed.setSpendGroup(authorisation.getSpendGroup());
        gpsMessageProcessed.setTransactionTimestamp(new Date().getTime());
        gpsMessageProcessed.setBlockedClientAmount(DecimalTypeUtils.toDecimal(earmarkAmount));
        gpsMessageProcessed.setBlockedClientCurrency(earmarkCurrency);
        if (settings != null) {
            gpsMessageProcessed.setInternalAccountCurrency(settings.getLinkedAccountCurrency());
            gpsMessageProcessed.setInternalAccountId(settings.getLinkedAccountId());
        }
        LOG.debug("Message generated. Parameters: {}", gpsMessageProcessed);
        return gpsMessageProcessed;
    }

    private Event generateEvent(String source, String eventName, Object eventData, String responseKey, String parentKey) {

        LOG.debug("Generating event with response data.");

        // Create the event type
        EventType eventType = new EventType();
        eventType.setName(eventName);
        eventType.setVersion(Constants.getDefaultEventVersion());
        eventType.setParentKey(Constants.EMPTY);
        eventType.setKey(responseKey);
        eventType.setSource(source);
        eventType.setParentKey(parentKey);
        SimpleDateFormat format = new SimpleDateFormat(Constants.getDefaultEventTimestampFormat());
        eventType.setTimestamp(format.format(new Date()));
        eventType.setData(eventData.toString());

        ProcessIdentifierType processIdentifier = new ProcessIdentifierType();
        processIdentifier.setUuid(responseKey);

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
