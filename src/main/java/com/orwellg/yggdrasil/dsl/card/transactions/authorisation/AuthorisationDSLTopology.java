package com.orwellg.yggdrasil.dsl.card.transactions.authorisation;

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

    private static final String TOPOLOGY_NAME = "dsl-card-authorisation";
    private static final String PROPERTIES_FILE = "authorisation-topology.properties";
    private static final String KAFKA_EVENT_READER = "kafka-event-reader";
    private static final String KAFKA_EVENT_SUCCESS_PROCESS = "kafka-event-success-process";
    private static final String KAFKA_EVENT_ERROR_PROCESS = "kafka-event-error-process";
    private static final String GET_DATA = "get-data";
    private static final String PROCESS_VALIDATION = "process-validation";
    private static final String RESPONSE_GENERATOR = "response-generator";
    private static final String KAFKA_EVENT_SUCCESS_PRODUCER = "kafka-event-success-producer";

    public static final String KAFKA_ERROR_PRODUCER_COMPONENT_ID = "get-kafka-error-producer";
    public static final String KAFKA_EVENT_READER_COMPONENT_ID = "get-kafka-event-reader";
    public static final String KAFKA_EVENT_ERROR_PROCESS_COMPONENT_ID = "get-kafka-event-error-process";

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
        GSpout kafkaEventReader = new GSpout(KAFKA_EVENT_READER, new KafkaSpoutWrapper(config.getKafkaSubscriberSpoutConfig(), String.class, String.class).getKafkaSpout(), config.getKafkaSpoutHints());

        // Parse the events and we send it to the rest of the topology
        GBolt<?> kafkaEventProcess = new GRichBolt(KAFKA_EVENT_SUCCESS_PROCESS, new EventToAuthorisationMessageBolt(), config.getEventProcessHints());
        kafkaEventProcess.addGrouping(new ShuffleGrouping(KAFKA_EVENT_READER, KafkaSpout.EVENT_SUCCESS_STREAM));

        // Get data from DB
        GBolt<?> getDataBolt = new GRichBolt(GET_DATA, new LoadDataBolt(GET_DATA), config.getActionBoltHints());
        getDataBolt.addGrouping(new ShuffleGrouping(KAFKA_EVENT_SUCCESS_PROCESS));

        // Validation bolt
        GBolt<?> processValidationBolt = new GRichBolt(PROCESS_VALIDATION, new ProcessJoinValidatorBolt(PROCESS_VALIDATION), config.getActionBoltHints());
        processValidationBolt.addGrouping(new ShuffleGrouping(GET_DATA));

        // Response generation bolt
        GBolt<?> responseGeneratorBolt = new GRichBolt(RESPONSE_GENERATOR, new ResponseGeneratorBolt(), config.getActionBoltHints());
        responseGeneratorBolt.addGrouping(new ShuffleGrouping(PROCESS_VALIDATION));

        // Send a event with the result
        GBolt<?> kafkaEventSuccessProducer = new GRichBolt(KAFKA_EVENT_SUCCESS_PRODUCER, new KafkaBoltWrapper(config.getKafkaPublisherBoltConfig(), String.class, String.class).getKafkaBolt(), config.getEventResponseHints());
        kafkaEventSuccessProducer.addGrouping(new ShuffleGrouping(RESPONSE_GENERATOR));

        ///////
        // GBolt for work with the errors
        GBolt<?> kafkaEventError = new GRichBolt(KAFKA_EVENT_ERROR_PROCESS_COMPONENT_ID, new EventErrorBolt(), config.getEventErrorHints());
        kafkaEventError
                .addGrouping(new ShuffleGrouping(KAFKA_EVENT_READER, KafkaSpout.EVENT_ERROR_STREAM));
        // GBolt for send errors of events to kafka
        KafkaBoltWrapper kafkaErrorBoltWrapper = new KafkaBoltWrapper(config.getKafkaPublisherErrorBoltConfig(), String.class, String.class);
        GBolt<?> kafkaErrorProducer = new GRichBolt(KAFKA_ERROR_PRODUCER_COMPONENT_ID,
                kafkaErrorBoltWrapper.getKafkaBolt(), config.getEventErrorHints());
        kafkaErrorProducer.addGrouping(new ShuffleGrouping(KAFKA_EVENT_ERROR_PROCESS_COMPONENT_ID));


        // Build the topology
        StormTopology topology = TopologyFactory.generateTopology(
                kafkaEventReader,
                Arrays.asList(kafkaEventProcess, kafkaEventError, kafkaErrorProducer, getDataBolt, processValidationBolt, responseGeneratorBolt, kafkaEventSuccessProducer));
        LOG.debug("Topology created");

        // Create the basic config and upload the topology
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(150);

        if (local) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, conf, topology);

            Thread.sleep(3000000);
            cluster.shutdown();
            ComponentFactory.getConfigurationParams().close();
        } else {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, topology);
        }
    }
}
