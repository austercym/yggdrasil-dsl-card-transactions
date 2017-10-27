package com.yggdrasil.dsl.card.transactions.topology.bolts.event;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.JoinFutureBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.yggdrasil.dsl.card.transactions.services.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ProcessJoinValidatorBolt extends JoinFutureBolt<Message> {

    private static final Logger LOG = LogManager.getLogger(ProcessJoinValidatorBolt.class);
    private AuthorisationValidator statusValidator;
    private AuthorisationValidator transactionTypeValidator;
    private AuthorisationValidator merchantValidator;

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
    }

    @Override
    protected void join(Tuple input, String key, String processId, Message eventData) {

        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        LOG.info("{}Previous starting processing the join validation for key {}", logPrefix, key);

        try {
            CardSettings settings = (CardSettings) input.getValueByField("retrieveValue");
            CompletableFuture<ValidationResult> statusFuture =
                    validate("Card status", statusValidator, eventData, settings, logPrefix);
            CompletableFuture<ValidationResult> transactionTypeFuture =
                    validate("Transaction type", transactionTypeValidator, eventData, settings, logPrefix);
            CompletableFuture<ValidationResult> merchantFuture =
                    validate("Merchant", merchantValidator, eventData, settings, logPrefix);

            CompletableFuture.allOf(statusFuture, transactionTypeFuture);

            Map<String, Object> values = new HashMap<>();
            values.put("key", key);
            values.put("processId", processId);
            values.put("eventData", eventData);
            values.put("retrieveValue", settings);
            values.put("statusValidationResult", statusFuture.get());
            values.put("transactionTypeValidationResult", transactionTypeFuture.get());
            values.put("merchantValidationResult", merchantFuture.get());

            send(input, values);

        } catch (Exception e) {
            LOG.error("{} Error processing the authorisation validation. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                "key", "processId", "eventData", "retrieveValue",
                "statusValidationResult", "transactionTypeValidationResult", "merchantValidationResult"));
    }

    private CompletableFuture<ValidationResult> validate(
            String name, AuthorisationValidator validator, Message message, CardSettings settings, String logPrefix) {
        return CompletableFuture.supplyAsync(
                () -> {
                    ValidationResult result = validator.validate(message, settings);
                    LOG.info("{}{} validation result: {}", logPrefix, name, result);
                    return result;
                });
    }
}
