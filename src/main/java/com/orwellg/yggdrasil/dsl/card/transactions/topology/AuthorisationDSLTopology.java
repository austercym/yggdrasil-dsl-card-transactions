package com.orwellg.yggdrasil.dsl.card.transactions.topology;

import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.storm.topology.TopologyFactory;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.EventErrorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GRichBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.grouping.ShuffleGrouping;
import com.orwellg.umbrella.commons.storm.topology.generic.spout.GSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.processors.authorisation.ProcessJoinValidatorBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.processors.authorisation.ResponseGeneratorBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.processors.authorisation.LoadDataBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.event.EventToAuthorisationMessageBolt;
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

    public static final String PROPERTIES_FILE = "authorisation-topology.properties";

    public static void main(String[] args) throws Exception {

        boolean local = false;
        if (args.length >= 1 && args[0].equals("local")) {
            local = true;
        }

        loadTopologyInStorm(local);
    }

    private static void loadTopologyInStorm(boolean local) throws Exception {
        LOG.info("Creating GPS authorisation message processing topology");

        // Read configuration params from authorisation-topology.properties and zookeeper
        TopologyConfig config = TopologyConfigFactory.getTopologyConfig(PROPERTIES_FILE);

        // Create the spout that read the events from Kafka
        GSpout kafkaEventReader = new GSpout("kafka-event-reader", new KafkaSpoutWrapper(config.getKafkaSubscriberSpoutConfig(), String.class, String.class).getKafkaSpout(), config.getKafkaSpoutHints());

        // Parse the events and we send it to the rest of the topology
        GBolt<?> kafkaEventProcess = new GRichBolt("kafka-event-success-process", new EventToAuthorisationMessageBolt(), config.getEventProcessHints());
        kafkaEventProcess.addGrouping(new ShuffleGrouping("kafka-event-reader", KafkaSpout.EVENT_SUCCESS_STREAM));

        // GBolt for work with the errors
        GBolt<?> kafkaEventError = new GRichBolt("kafka-event-error-process", new EventErrorBolt(), config.getEventErrorHints());
        kafkaEventError.addGrouping(new ShuffleGrouping("kafka-event-reader", KafkaSpout.EVENT_ERROR_STREAM));

        // GBolt for send errors of events to kafka
        GBolt<?> kafkaErrorProducer = new GRichBolt("kafka-error-producer", new KafkaBoltWrapper(config.getKafkaPublisherErrorBoltConfig(), String.class, String.class).getKafkaBolt(), config.getEventErrorHints());
        kafkaErrorProducer.addGrouping(new ShuffleGrouping("kafka-event-error-process"));

        // Get data from DB
        GBolt<?> getDataBolt = new GRichBolt("get-data", new LoadDataBolt("get-data"), config.getActionBoltHints());
        getDataBolt.addGrouping(new ShuffleGrouping("kafka-event-success-process"));

        // Validation bolt
        GBolt<?> processValidationBolt = new GRichBolt("process-validation", new ProcessJoinValidatorBolt("process-validation"), config.getActionBoltHints());
        processValidationBolt.addGrouping(new ShuffleGrouping("get-data"));

        // Response generation bolt
        GBolt<?> sampleBolt = new GRichBolt("response-generator", new ResponseGeneratorBolt(), config.getActionBoltHints());
        sampleBolt.addGrouping(new ShuffleGrouping("process-validation"));

        // Send a event with the result
        GBolt<?> kafkaEventSuccessProducer = new GRichBolt("kafka-event-success-producer", new KafkaBoltWrapper(config.getKafkaPublisherBoltConfig(), String.class, String.class).getKafkaBolt(), config.getEventResponseHints());
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
