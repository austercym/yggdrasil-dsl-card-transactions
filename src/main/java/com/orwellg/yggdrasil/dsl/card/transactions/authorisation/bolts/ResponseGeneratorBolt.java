package com.orwellg.yggdrasil.dsl.card.transactions.authorisation.bolts;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.AccountBalance;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.yggdrasil.card.transaction.commons.authorisation.services.AuthorisationResponseGenerator;
import com.orwellg.yggdrasil.card.transaction.commons.authorisation.services.AuthorisationValidationResults;
import com.orwellg.yggdrasil.card.transaction.commons.authorisation.services.ValidationResult;
import com.orwellg.yggdrasil.card.transaction.commons.model.TransactionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ResponseGeneratorBolt extends BasicRichBolt {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(ResponseGeneratorBolt.class);

    private AuthorisationResponseGenerator authorisationResponseGenerator;

    void setAuthorisationResponseGenerator(AuthorisationResponseGenerator authorisationResponseGenerator) {
        this.authorisationResponseGenerator = authorisationResponseGenerator;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        authorisationResponseGenerator = new AuthorisationResponseGenerator();
    }

    @Override
    public void execute(Tuple input) {

        long startTime = System.currentTimeMillis();
        LOG.info("Preparing response for input={}", input);

        try {
            String processId = input.getStringByField(Fields.PROCESS_ID);
            TransactionInfo event = (TransactionInfo) input.getValueByField(Fields.EVENT_DATA);
            String parentKey = (String) input.getValueByField(Fields.KEY);
            CardSettings settings = (CardSettings) input.getValueByField(Fields.CARD_SETTINGS);
            String transactionId = input.getStringByField(Fields.TRANSACTION_ID);
            AccountBalance accountBalance =
                    (AccountBalance) input.getValueByField(Fields.ACCOUNT_BALANCE);
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
                    parentKey, event.getProviderTransactionId(), event.getProviderMessageId(),
                    event.getDebitCardId(), event.getCardToken(),
                    event.getTransactionAmount(), event.getTransactionCurrency());
            LOG.debug("{}Generating response for authorisation message...", logPrefix);

            AuthorisationValidationResults validationResults = new AuthorisationValidationResults();
            validationResults.setBalanceValidationResult(balanceValidationResult);
            validationResults.setMerchantValidationResult(merchantValidationResult);
            validationResults.setStatusValidationResult(statusValidationResult);
            validationResults.setTransactionTypeValidationResult(transactionTypeValidationResult);
            validationResults.setVelocityLimitsValidationResult(velocityLimitsValidationResult);
            MessageProcessed processedMessage = authorisationResponseGenerator.getMessageProcessed(
                    transactionId, event, settings, accountBalance, validationResults);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, input.getStringByField(Fields.KEY));
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_NAME, CardTransactionEvents.RESPONSE_MESSAGE.getEventName());
            values.put(Fields.RESULT, processedMessage);
            send(input, values);

            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            LOG.info(
                    "{}Response code for authorisation message: {} ({})). (Execution time: {} ms)",
                    logPrefix,
                    processedMessage.getEhiResponse().getResponsestatus(),
                    processedMessage.getEhiResponse().getResponsestatus(),
                    elapsedTime);

        } catch (Exception e) {
            LOG.error("Response generation failed - input={}, message={}", input, e.getMessage(), e);
            error(e, input);
        }
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_NAME, Fields.RESULT));
    }
}
