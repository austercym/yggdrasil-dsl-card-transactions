package com.orwellg.yggdrasil.dsl.card.transactions.authorisationreversal;

import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.storm.topology.TopologyFactory;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.EventErrorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaEventGeneratorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GRichBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.grouping.ShuffleGrouping;
import com.orwellg.umbrella.commons.storm.topology.generic.spout.GSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.orwellg.yggdrasil.dsl.card.transactions.authorisationreversal.bolts.GenerateProcessedMessageBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.authorisationreversal.bolts.LoadDataBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.common.bolts.EventToTransactionInfoBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;

import java.util.Arrays;

public class AuthorisationReversalDSLTopology {

    private final static Logger LOG = LogManager.getLogger(AuthorisationReversalDSLTopology.class);

    private static final String TOPOLOGY_NAME = "dsl-card-authorisation-reversal";
    private static final String PROPERTIES_FILE = "authorisation-reversal-topology.properties";
    private static final String KAFKA_EVENT_READER = "kafka-event-reader";
    private static final String KAFKA_EVENT_SUCCESS_PROCESS = "kafka-event-success-process";
    private static final String GET_DATA = "get-data";
    private static final String PROCESSED_MESSAGE_GENERATION = "processed-message-generation";
    private static final String EVENT_GENERATOR = "authorisationReversalEventGenerator";
    private static final String KAFKA_EVENT_SUCCESS_PRODUCER = "kafka-event-success-producer";

    public static final String KAFKA_ERROR_PRODUCER_COMPONENT_ID = "get-kafka-error-producer";
    public static final String KAFKA_EVENT_ERROR_PROCESS_COMPONENT_ID = "get-kafka-event-error-process";

    public static void main(String[] args) throws Exception {

        boolean local = false;
        if (args.length >= 1 && args[0].equals("local")) {
            local = true;
        }

        loadTopologyInStorm(local);
    }

    private static void loadTopologyInStorm(boolean local) throws Exception {
        LOG.debug("Creating GPS authorisation reversal message processing topology");

        TopologyConfig config = TopologyConfigFactory.getTopologyConfig(PROPERTIES_FILE);

        // Create the spout that read the events from Kafka
        GSpout kafkaEventReader = new GSpout(KAFKA_EVENT_READER, new KafkaSpoutWrapper(config.getKafkaSubscriberSpoutConfig(), String.class, String.class).getKafkaSpout(), config.getKafkaSpoutHints());

        // Parse the events and we send it to the rest of the topology
        GBolt<?> kafkaEventProcess = new GRichBolt(KAFKA_EVENT_SUCCESS_PROCESS, new EventToTransactionInfoBolt(), config.getEventProcessHints());
        kafkaEventProcess.addGrouping(new ShuffleGrouping(KAFKA_EVENT_READER, KafkaSpout.EVENT_SUCCESS_STREAM));

        // Get data from DB
        GBolt<?> getDataBolt = new GRichBolt(GET_DATA, new LoadDataBolt(GET_DATA), config.getActionBoltHints());
        getDataBolt.addGrouping(new ShuffleGrouping(KAFKA_EVENT_SUCCESS_PROCESS));

        // Generate processed message
        GBolt<?> processedMessageBolt = new GRichBolt(PROCESSED_MESSAGE_GENERATION, new GenerateProcessedMessageBolt(), config.getActionBoltHints());
        processedMessageBolt.addGrouping(new ShuffleGrouping(GET_DATA));

        GBolt<?> eventGeneratorBolt = new GRichBolt(EVENT_GENERATOR, new KafkaEventGeneratorBolt(), config.getActionBoltHints());
        eventGeneratorBolt.addGrouping(new ShuffleGrouping(PROCESSED_MESSAGE_GENERATION));

        // Send a event with the result
        GBolt<?> kafkaEventSuccessProducer = new GRichBolt(KAFKA_EVENT_SUCCESS_PRODUCER, new KafkaBoltWrapper(config.getKafkaPublisherBoltConfig(), String.class, String.class).getKafkaBolt(), config.getEventResponseHints());
        kafkaEventSuccessProducer.addGrouping(new ShuffleGrouping(PROCESSED_MESSAGE_GENERATION));

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
                Arrays.asList(kafkaEventProcess, kafkaEventError, kafkaErrorProducer, getDataBolt, processedMessageBolt, eventGeneratorBolt, kafkaEventSuccessProducer));
        LOG.info("GPS Authorisation reversal message processing topology created");

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