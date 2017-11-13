package com.orwellg.yggdrasil.dsl.card.transactions.presentment;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class ProcessExceptionBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(ProcessExceptionBolt.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(new String[] {"key", "message"}));
    }

    @Override
    public void execute(Tuple tuple) {

        String key = tuple.getStringByField("key");
        LOG.info("[Key: {}] Error received: {}. Getting the original event value.", key, tuple);

        try {

            String processId =  tuple.getStringByField("processId");
            Message message = (Message) tuple.getValueByField("eventData");
            String exceptionMessage = tuple.getStringByField("exceptionMessage");
            String exceptionStackTrace = tuple.getStringByField("exceptionStackTrace");

            StringBuilder builder = new StringBuilder();
            builder.append("[processId: ");
            builder.append(processId);
            builder.append("][eventData: ");
            builder.append(message);
            builder.append("][exceptionMessage: ");
            builder.append(exceptionMessage);
            builder.append("][exceptionStackTrace: ");
            builder.append(exceptionStackTrace);
            String kafkaMessage = builder.toString();

            Map<String, Object> values = new HashMap<>();
            values.put("key", key);
            values.put("message", kafkaMessage);

            send(tuple, values);

            LOG.info("[Key: {}] Original event, exception message and stack trace send to correspondent Kafka topic: {}.", key, values);
        } catch (Exception e) {
            LOG.error("[Key: {}] Error when the system tries send the original event {} to error topic. Message: {}", key, tuple, e.getMessage(), e);
            error(e, tuple);
        }
    }
}
