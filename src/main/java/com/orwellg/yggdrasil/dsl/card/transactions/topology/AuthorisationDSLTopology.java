package com.orwellg.yggdrasil.dsl.card.transactions.topology;

import com.orwellg.umbrella.commons.storm.topology.TopologyFactory;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.EventErrorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GRichBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.grouping.ShuffleGrouping;
import com.orwellg.umbrella.commons.storm.topology.generic.spout.GSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltFieldNameWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.event.KafkaEventProcessBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.event.ProcessJoinValidatorBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.event.ResponseGeneratorBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.event.LoadDataBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;

import java.util.Arrays;

public class AuthorisationDSLTopology {

    private final static Logger LOG = LogManager.getLogger(AuthorisationDSLTopology.class);


    public static void main(String[] args) throws Exception {

        boolean local = false;
        if (args.length >= 1 && args[0].equals("local")) {
            local = true;
        }

        loadTopologyInStorm(local);
    }

    private static void loadTopologyInStorm(boolean local) throws Exception {
        LOG.debug("Creating GPS message processing topology");

        Integer hints = ComponentFactory.getConfigurationParams().getTopologyParams().getHints();

        // Create the spout that read the events from Kafka
        GSpout kafkaEventReader = new GSpout("kafka-event-reader", new KafkaSpoutWrapper("subscriber-gps-authorisation-dsl-topic.yaml", String.class, String.class).getKafkaSpout(), hints);

        // Parse the events and we send it to the rest of the topology
        GBolt<?> kafkaEventProcess = new GRichBolt("kafka-event-success-process", new KafkaEventProcessBolt(), hints);
        kafkaEventProcess.addGrouping(new ShuffleGrouping("kafka-event-reader", KafkaSpout.EVENT_SUCCESS_STREAM));

        // GBolt for work with the errors
        GBolt<?> kafkaEventError = new GRichBolt("kafka-event-error-process", new EventErrorBolt(), hints);
        kafkaEventError.addGrouping(new ShuffleGrouping("kafka-event-reader", KafkaSpout.EVENT_ERROR_STREAM));

        // GBolt for send errors of events to kafka
        GBolt<?> kafkaErrorProducer = new GRichBolt("kafka-error-producer", new KafkaBoltWrapper("publisher-gps-dsl-error.yaml", String.class, String.class).getKafkaBolt(), hints);
        kafkaErrorProducer.addGrouping(new ShuffleGrouping("kafka-event-error-process"));

        // Get data from DB
        GBolt<?> getDataBolt = new GRichBolt("get-data", new LoadDataBolt("get-data"), hints);
        getDataBolt.addGrouping(new ShuffleGrouping("kafka-event-success-process"));

        // Validation bolt
        GBolt<?> processValidationBolt = new GRichBolt("process-validation", new ProcessJoinValidatorBolt("process-validation"), 20);
        processValidationBolt.addGrouping(new ShuffleGrouping("get-data"));

        // Response generation bolt
        GBolt<?> sampleBolt = new GRichBolt("response-generator", new ResponseGeneratorBolt(), hints);
        sampleBolt.addGrouping(new ShuffleGrouping("process-validation"));

        // Send a event with the result
        GBolt<?> kafkaEventSuccessProducer = new GRichBolt("kafka-event-success-producer", new KafkaBoltFieldNameWrapper("publisher-gps-dsl-result-success.yaml", String.class, String.class).getKafkaBolt(), 10);
        kafkaEventSuccessProducer.addGrouping(new ShuffleGrouping("response-generator"));

        // Build the topology
        StormTopology topology = TopologyFactory.generateTopology(
                kafkaEventReader,
                Arrays.asList(kafkaEventProcess, kafkaEventError, kafkaErrorProducer, getDataBolt, processValidationBolt, sampleBolt, kafkaEventSuccessProducer));
        LOG.debug("Topology created");

        // Create the basic config and upload the topology
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(150);

        if (local) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("dsl-gps-authorisation", conf, topology);

            Thread.sleep(3000000);
            cluster.shutdown();
            ComponentFactory.getConfigurationParams().close();
        } else {
            StormSubmitter.submitTopology("dsl-gps-authorisation", conf, topology);
        }
    }
}
