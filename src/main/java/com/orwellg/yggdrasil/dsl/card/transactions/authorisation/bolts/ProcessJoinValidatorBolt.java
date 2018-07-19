package com.orwellg.yggdrasil.dsl.card.transactions.authorisation.bolts;

import com.orwellg.umbrella.commons.storm.topology.component.bolt.JoinFutureBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.AccountBalance;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.yggdrasil.card.transaction.commons.authorisation.services.AuthorisationValidationService;
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

public class ProcessJoinValidatorBolt extends JoinFutureBolt<TransactionInfo> {

    private static final Logger LOG = LogManager.getLogger(ProcessJoinValidatorBolt.class);

    private AuthorisationValidationService authorisationValidationService;

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
        authorisationValidationService = new AuthorisationValidationService();
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
            String transactionId = input.getStringByField(Fields.TRANSACTION_ID);

            ValidationResult statusValidationResult =
                    authorisationValidationService.validateCardStatus(key, eventData, settings);
            ValidationResult transactionTypeValidationResult =
                    eventData.getIsBalanceEnquiry()
                            ? null
                            : authorisationValidationService.validateTransactionType(key, eventData, settings);
            ValidationResult merchantValidationResult =
                    eventData.getIsBalanceEnquiry()
                            ? null
                            : authorisationValidationService.validateMerchant(key, eventData, settings);
            ValidationResult velocityLimitsValidationResult =
                    eventData.getIsBalanceEnquiry()
                            ? null
                            : authorisationValidationService.validateVelocityLimits(key, eventData, settings, totalAmounts);
            ValidationResult balanceValidationResult =
                    eventData.getIsBalanceEnquiry()
                            ? null
                            : authorisationValidationService.validateBalance(key, eventData, accountBalance);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_DATA, eventData);
            values.put(Fields.CARD_SETTINGS, settings);
            values.put(Fields.ACCOUNT_BALANCE, accountBalance);
            values.put(Fields.TRANSACTION_ID, transactionId);
            values.put(Fields.STATUS_VALIDATION_RESULT, statusValidationResult);
            values.put(Fields.TRANSACTION_TYPE_VALIDATION_RESULT, transactionTypeValidationResult);
            values.put(Fields.MERCHANT_VALIDATION_RESULT, merchantValidationResult);
            values.put(Fields.VELOCITY_LIMITS_VALIDATION_RESULT, velocityLimitsValidationResult);
            values.put(Fields.BALANCE_VALIDATION_RESULT, balanceValidationResult);

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
                Fields.TRANSACTION_ID,
                Fields.STATUS_VALIDATION_RESULT, Fields.TRANSACTION_TYPE_VALIDATION_RESULT,
                Fields.MERCHANT_VALIDATION_RESULT, Fields.VELOCITY_LIMITS_VALIDATION_RESULT,
                Fields.BALANCE_VALIDATION_RESULT));
    }
}
