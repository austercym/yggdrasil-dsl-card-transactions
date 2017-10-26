package com.yggdrasil.dsl.card.transactions.topology.bolts.event;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.JoinFutureBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.yggdrasil.dsl.card.transactions.services.AuthorisationValidator;
import com.yggdrasil.dsl.card.transactions.services.StatusValidator;
import com.yggdrasil.dsl.card.transactions.services.TransactionTypeValidator;
import com.yggdrasil.dsl.card.transactions.services.ValidationResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ProcessJoinValidatorBolt extends JoinFutureBolt<Message> {

    private static final Logger LOG = LogManager.getLogger(ProcessJoinValidatorBolt.class);

    @Override
    public String getEventSuccessStream() {
        return KafkaSpout.EVENT_SUCCESS_STREAM;
    }

    @Override
    public String getEventErrorStream() {
        return KafkaSpout.EVENT_ERROR_STREAM;
    }

    public ProcessJoinValidatorBolt(String joinId) {
        super(joinId);
    }

    @Override
    protected void join(Tuple input, String key, String processId, Message eventData) {

        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        LOG.info("{}Previous starting processing the join validation for key {}", logPrefix, key);

        try {
            CardSettings settings = (CardSettings) input.getValueByField("retrieveValue");
            CompletableFuture<ValidationResult> statusFuture = CompletableFuture.supplyAsync(
                    () -> validateStatus(eventData, settings, logPrefix));
            CompletableFuture<ValidationResult> transactionTypeFuture = CompletableFuture.supplyAsync(
                    () -> validateTransactionType(eventData, settings, logPrefix));

            CompletableFuture.allOf(statusFuture, transactionTypeFuture);

            Map<String, Object> values = new HashMap<>();
            values.put("key", key);
            values.put("processId", processId);
            values.put("eventData", eventData);
            values.put("retrieveValue", settings);
            values.put("statusValidationResult", statusFuture.get());
            values.put("transactionTypeValidationResult", transactionTypeFuture.get());

            send(input, values);

        } catch (Exception e) {
            LOG.error("{} Error processing the authorisation validation. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                "key", "processId", "eventData", "retrieveValue", "statusValidationResult", "transactionTypeValidationResult"));
    }

    private ValidationResult validateStatus(Message message, CardSettings settings, String logPrefix) {
        AuthorisationValidator statusValidation = new StatusValidator();
        ValidationResult result = statusValidation.validate(message, settings);
        LOG.info("{}Card status validation result: {}", logPrefix, result);
        return result;
    }

    private ValidationResult validateTransactionType(Message message, CardSettings settings, String logPrefix) {
        AuthorisationValidator statusValidation = new TransactionTypeValidator();
        ValidationResult result = statusValidation.validate(message, settings);
        LOG.info("{}Transaction type validation result: {}", logPrefix, result);
        return result;
    }
}
