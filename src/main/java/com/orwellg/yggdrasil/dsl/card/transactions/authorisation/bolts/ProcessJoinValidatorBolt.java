package com.orwellg.yggdrasil.dsl.card.transactions.authorisation.bolts;

import com.orwellg.umbrella.commons.storm.topology.component.bolt.JoinFutureBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.AccountBalance;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services.*;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ProcessJoinValidatorBolt extends JoinFutureBolt<TransactionInfo> {

    private static final Logger LOG = LogManager.getLogger(ProcessJoinValidatorBolt.class);

    private AuthorisationValidator statusValidator;
    private AuthorisationValidator transactionTypeValidator;
    private AuthorisationValidator merchantValidator;
    private VelocityLimitsValidator velocityLimitsValidator;
    private BalanceValidator balanceValidator;

    public ProcessJoinValidatorBolt(String joinId) {
        super(joinId);
    }

    @Override
    public String getEventSuccessStream() {
        return KafkaSpout.EVENT_SUCCESS_STREAM;
    }

    @Override
    public String getEventErrorStream() {
        return KafkaSpout.EVENT_ERROR_STREAM;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        statusValidator = new StatusValidator();
        transactionTypeValidator = new TransactionTypeValidator();
        merchantValidator = new MerchantValidator();
        velocityLimitsValidator = new VelocityLimitsValidator();
        balanceValidator = new BalanceValidator();
    }

    @Override
    protected void join(Tuple input, String key, String processId, TransactionInfo eventData) {

        long startTime = System.currentTimeMillis();
        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        LOG.info("{}Starting processing the join validation for key {}", logPrefix, key);

        try {
            CardSettings settings = (CardSettings) input.getValueByField(Fields.CARD_SETTINGS);
            SpendingTotalAmounts totalAmounts = (SpendingTotalAmounts) input.getValueByField(Fields.SPENDING_TOTALS);
            AccountBalance accountBalance = (AccountBalance) input.getValueByField(Fields.ACCOUNT_BALANCE);

            CompletableFuture<ValidationResult> statusFuture =
                    validate("Card status", statusValidator, eventData, settings, logPrefix);
            CompletableFuture<ValidationResult> transactionTypeFuture =
                    eventData.getIsBalanceEnquiry()
                            ? CompletableFuture.completedFuture(null)
                            : validate("Transaction type", transactionTypeValidator, eventData, settings, logPrefix);
            CompletableFuture<ValidationResult> merchantFuture =
                    eventData.getIsBalanceEnquiry()
                            ? CompletableFuture.completedFuture(null)
                            : validate("Merchant", merchantValidator, eventData, settings, logPrefix);
            CompletableFuture<ValidationResult> velocityLimitsFuture =
                    eventData.getIsBalanceEnquiry()
                            ? CompletableFuture.completedFuture(null)
                            : validateVelocityLimits(eventData, settings, totalAmounts, logPrefix);
            CompletableFuture<ValidationResult> balanceFuture =
                    eventData.getIsBalanceEnquiry()
                            ? CompletableFuture.completedFuture(null)
                            : validateBalance(eventData, accountBalance, logPrefix);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_DATA, eventData);
            values.put(Fields.CARD_SETTINGS, settings);
            values.put(Fields.ACCOUNT_BALANCE, accountBalance);
            values.put(Fields.STATUS_VALIDATION_RESULT, statusFuture.get());
            values.put(Fields.TRANSACTION_TYPE_VALIDATION_RESULT, transactionTypeFuture.get());
            values.put(Fields.MERCHANT_VALIDATION_RESULT, merchantFuture.get());
            values.put(Fields.VELOCITY_LIMITS_VALIDATION_RESULT, velocityLimitsFuture.get());
            values.put(Fields.BALANCE_VALIDATION_RESULT, balanceFuture.get());

            send(input, values);

            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            LOG.info("{}Processed the join validation for key {}. (Execution time: {} ms)", logPrefix, key, elapsedTime);
        } catch (Exception e) {
            LOG.error("{} Error processing the authorisation validation. Message: {}", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA, Fields.CARD_SETTINGS, Fields.ACCOUNT_BALANCE,
                Fields.STATUS_VALIDATION_RESULT, Fields.TRANSACTION_TYPE_VALIDATION_RESULT,
                Fields.MERCHANT_VALIDATION_RESULT, Fields.VELOCITY_LIMITS_VALIDATION_RESULT,
                Fields.BALANCE_VALIDATION_RESULT));
    }

    private CompletableFuture<ValidationResult> validate(
            String name, AuthorisationValidator validator, TransactionInfo message, CardSettings settings, String logPrefix) {
        return CompletableFuture.supplyAsync(
                () -> {
                    ValidationResult result = validator.validate(message, settings);
                    LOG.info("{}{} validation result: {}", logPrefix, name, result);
                    return result;
                });
    }

    private CompletableFuture<ValidationResult> validateVelocityLimits(
            TransactionInfo message, CardSettings settings, SpendingTotalAmounts totalCurrent, String logPrefix) {
        return CompletableFuture.supplyAsync(
                () -> {
                    ValidationResult result = velocityLimitsValidator.validate(
                            message, settings, totalCurrent);
                    LOG.info("{}Velocity limits validation result: {}", logPrefix, result);
                    return result;
                });
    }

    private CompletableFuture<ValidationResult> validateBalance(
            TransactionInfo message, AccountBalance accountBalance, String logPrefix) {
        return CompletableFuture.supplyAsync(
                () -> {
                    ValidationResult result = balanceValidator.validate(
                            message, accountBalance);
                    LOG.info("{}Account balance validation result: {}", logPrefix, result);
                    return result;
                });
    }
}
