package com.orwellg.yggdrasil.dsl.card.transactions.authorisation.bolts;

import com.orwellg.umbrella.avro.types.cards.CardMessageProcessed;
import com.orwellg.umbrella.avro.types.gps.ResponseMsg;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.accounting.AccountTransactionLog;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.ResponseCode;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services.ValidationResult;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.CardMessageProcessedFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ResponseGeneratorBolt extends BasicRichBolt {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(ResponseGeneratorBolt.class);

    @Override
    public void execute(Tuple input) {

        LOG.debug("Preparing response for input={}", input);

        try {
            String processId = input.getStringByField(Fields.PROCESS_ID);
            TransactionInfo event = (TransactionInfo) input.getValueByField(Fields.EVENT_DATA);
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
            if (event.getIsBalanceEnquiry()) {
                if (statusValidationResult.getIsValid()) {
                    responseCode = ResponseCode.ALL_GOOD;
                    if (accountTransactionLog != null) {
                        response.setAvlBalance(accountTransactionLog.getActualBalance() == null
                                ? 0
                                : accountTransactionLog.getActualBalance().doubleValue());
                        response.setCurBalance(accountTransactionLog.getLedgerBalance() == null
                                ? 0
                                : accountTransactionLog.getLedgerBalance().doubleValue());
                    }
                }
            } else if (!balanceValidationResult.getIsValid()) {
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
                    response.setAvlBalance(Optional.ofNullable(accountTransactionLog.getActualBalance())
                            .orElse(BigDecimal.ZERO)
                            .subtract(event.getSettlementAmount().abs())
                            .doubleValue());
                    response.setCurBalance(Optional.ofNullable(accountTransactionLog.getLedgerBalance())
                            .orElse(BigDecimal.ZERO)
                            .doubleValue());
                }
            }

            response.setAcknowledgement("1");
            response.setResponsestatus(responseCode.getCode());

            CardMessageProcessed processedMessage = generateMessageProcessed(event, response, settings, earmarkAmount, earmarkCurrency);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, input.getStringByField(Fields.KEY));
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_NAME, CardTransactionEvents.RESPONSE_MESSAGE.getEventName());
            values.put(Fields.RESULT, processedMessage);
            send(input, values);

            LOG.info(
                    "{}Response code for authorisation message: {} ({}))",
                    logPrefix, response.getResponsestatus(), responseCode);

        } catch (Exception e) {
            LOG.error("Response generation failed - input={}, message={}", input, e.getMessage(), e);
            error(e, input);
        }
    }

    private CardMessageProcessed generateMessageProcessed(TransactionInfo authorisation, ResponseMsg response, CardSettings settings, BigDecimal earmarkAmount, String earmarkCurrency) {

        LOG.debug("Generating CardMessageProcessed message");
        CardMessageProcessed CardMessageProcessed = CardMessageProcessedFactory.from(authorisation);
        CardMessageProcessed.setEhiResponse(response);
        CardMessageProcessed.setEarmarkAmount(DecimalTypeUtils.toDecimal(earmarkAmount));
        CardMessageProcessed.setEarmarkCurrency(earmarkCurrency);
        CardMessageProcessed.setTotalEarmarkAmount(DecimalTypeUtils.toDecimal(earmarkAmount));
        CardMessageProcessed.setTotalEarmarkCurrency(earmarkCurrency);
        CardMessageProcessed.setClientAmount(DecimalTypeUtils.toDecimal(0));
        CardMessageProcessed.setClientCurrency(earmarkCurrency);
        CardMessageProcessed.setTotalClientAmount(DecimalTypeUtils.toDecimal(0));
        CardMessageProcessed.setTotalClientCurrency(earmarkCurrency);
        CardMessageProcessed.setWirecardAmount(DecimalTypeUtils.toDecimal(0));
        CardMessageProcessed.setWirecardCurrency(authorisation.getSettlementCurrency());
        CardMessageProcessed.setTotalWirecardAmount(DecimalTypeUtils.toDecimal(0));
        CardMessageProcessed.setTotalWirecardCurrency(authorisation.getSettlementCurrency());
        if (settings != null) {
            CardMessageProcessed.setInternalAccountCurrency(settings.getLinkedAccountCurrency());
            CardMessageProcessed.setInternalAccountId(settings.getLinkedAccountId());
        }
        LOG.debug("Message generated. Parameters: {}", CardMessageProcessed);
        return CardMessageProcessed;
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_NAME, Fields.RESULT));
    }
}