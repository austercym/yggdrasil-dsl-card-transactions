package com.yggdrasil.dsl.card.transactions.topology.bolts.processors.presentment;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class GpsErrorBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(GpsErrorBolt.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(new String[] {"key", "message"}));
    }

    @Override
    public void execute(Tuple tuple) {

        String key = tuple.getStringByField("key");
        LOG.info("[Key: {}] Error received: {}. Getting the original event value.", key, tuple);

        try {

            Message message = (Message) tuple.getValueByField("eventData");

            Map<String, Object> values = new HashMap<>();
            values.put("key", key);
            values.put("message", message);

            send(tuple, values);

            LOG.info("[Key: {}] Original event send to correspondent Kafka topic: {}.", key, values);
        } catch (Exception e) {
            LOG.error("[Key: {}] Error when the system tries send the original event {} to error topic. Message: {}", key, tuple, e.getMessage(), e);
            error(e, tuple);
        }
    }
}
