package com.yggdrasil.dsl.card.transactions.topology.bolts.event;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.JoinFutureBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.ResponseCode;
import com.yggdrasil.dsl.card.transactions.services.AuthorisationValidationService;
import com.yggdrasil.dsl.card.transactions.services.StatusValidationServiceImpl;
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

    /**
     * Define the name of the success stream
     */
    public static final String EVENT_ACCEPTED_STREAM = "validation-authorisation-accepted-stream";
    /**
     * Define the name of the error stream
     */
    public static final String EVENT_DECLINED_STREAM = "validation-authorisation-declined-stream";

    @Override
    public String getEventSuccessStream() {
        return EVENT_ACCEPTED_STREAM;
    }

    @Override
    public String getEventErrorStream() {
        return EVENT_DECLINED_STREAM;
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
            AuthorisationValidationService statusValidation = new StatusValidationServiceImpl();
            CompletableFuture<ValidationResult> statusFuture = CompletableFuture.supplyAsync(
                    () -> statusValidation.validate(eventData, settings));
            ValidationResult statusResult = statusFuture.get();

            LOG.debug("{}Card status validation result: {}", logPrefix, statusResult);

            ResponseCode responseCode = ResponseCode.DO_NOT_HONOUR;
            String stream = getEventErrorStream();
            if (statusResult.getIsValid())
            {
                responseCode = ResponseCode.ALL_GOOD;
                stream = getEventSuccessStream();
            }

            Map<String, Object> values = new HashMap<>();
            values.put("key", key);
            values.put("processId", processId);
            values.put("eventData", eventData);
            values.put("retrieveValue", settings);
            values.put("validationResult", responseCode);

            send(stream, input, values);

        } catch (Exception e) {
            LOG.error("{} Error processing the authorisation validation. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(EVENT_ACCEPTED_STREAM, Arrays.asList("key", "processId", "eventData", "retrieveValue", "validationResult"));
        addFielsDefinition(EVENT_DECLINED_STREAM, Arrays.asList("key", "processId", "eventData", "retrieveValue", "validationResult"));
    }
}
