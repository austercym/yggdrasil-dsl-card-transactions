package com.orwellg.yggdrasil.dsl.card.transactions.externallyauthorised;

import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.storm.topology.TopologyFactory;
import com.orwellg.umbrella.commons.storm.topology.component.base.AbstractTopology;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.EventErrorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaEventGeneratorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GRichBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.grouping.ShuffleGrouping;
import com.orwellg.umbrella.commons.storm.topology.generic.spout.GSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.orwellg.yggdrasil.dsl.card.transactions.common.bolts.EventToTransactionInfoBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.common.bolts.LoadTransactionListBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.externallyauthorised.bolts.GenerateProcessedMessageBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.generated.StormTopology;

import java.util.Arrays;

public class ExternallyAuthorisedTopology extends AbstractTopology {

    public static final String PROPERTIES_FILE = "externally-authorised-topology.properties";
    private static final Logger LOG = LogManager.getLogger(ExternallyAuthorisedTopology.class);
    private static final String TOPOLOGY_NAME = "yggdrasil-card-externally-authorised";
    private static final String BOLT_NAME_PREFIX = "externallyAuthorised";
    private static final String KAFKA_EVENT_READER_COMPONENT = BOLT_NAME_PREFIX + "Reader";
    private static final String PROCESS_COMPONENT = BOLT_NAME_PREFIX + "Process";
    private static final String GET_DATA = BOLT_NAME_PREFIX + "GetData";
    private static final String PROCESSED_MESSAGE_GENERATION = BOLT_NAME_PREFIX + "ProcessedMessageGeneration";
    private static final String EVENT_GENERATOR = BOLT_NAME_PREFIX + "EventGenerator";
    private static final String KAFKA_EVENT_SUCCESS_PRODUCER = BOLT_NAME_PREFIX + "KafkaEventSuccessProducer";
    private static final String ERROR_HANDLING = BOLT_NAME_PREFIX + "ErrorHandling";
    private static final String ERROR_PRODUCER_COMPONENT = BOLT_NAME_PREFIX + "ErrorProducer";

    @Override
    public StormTopology load() {
        return load(null);
    }

    @Override
    public StormTopology load(String zookeeperHost) {
        // Read configuration params from the topology properties file and zookeeper
        TopologyConfig config = TopologyConfigFactory.getTopologyConfig(PROPERTIES_FILE, zookeeperHost);

        // -------------------------------------------------------
        // Create the spout that read events from Kafka
        // -------------------------------------------------------
        GSpout kafkaEventReader = new GSpout(KAFKA_EVENT_READER_COMPONENT,
                new KafkaSpoutWrapper(config.getKafkaSubscriberSpoutConfig(), String.class, String.class).getKafkaSpout(),
                config.getKafkaSpoutHints());

        // -------------------------------------------------------
        // Process events
        // -------------------------------------------------------
        GBolt<?> processBolt = new GRichBolt(PROCESS_COMPONENT, new EventToTransactionInfoBolt(),
                config.getActionBoltHints());
        processBolt.addGrouping(new ShuffleGrouping(KAFKA_EVENT_READER_COMPONENT, KafkaSpout.EVENT_SUCCESS_STREAM));

        // Get data from DB
        GBolt<?> getDataBolt = new GRichBolt(GET_DATA, new LoadTransactionListBolt(PROPERTIES_FILE), config.getActionBoltHints());
        getDataBolt.addGrouping(new ShuffleGrouping(PROCESS_COMPONENT));

        // Generate processed message
        GBolt<?> processedMessageBolt = new GRichBolt(PROCESSED_MESSAGE_GENERATION, new GenerateProcessedMessageBolt(), config.getActionBoltHints());
        processedMessageBolt.addGrouping(new ShuffleGrouping(GET_DATA));

        GBolt<?> eventGeneratorBolt = new GRichBolt(EVENT_GENERATOR, new KafkaEventGeneratorBolt(), config.getActionBoltHints());
        eventGeneratorBolt.addGrouping(new ShuffleGrouping(PROCESSED_MESSAGE_GENERATION));

        // Send a event with the result
        GBolt<?> kafkaEventSuccessProducer = new GRichBolt(KAFKA_EVENT_SUCCESS_PRODUCER, new KafkaBoltWrapper(config.getKafkaPublisherBoltConfig(), String.class, String.class).getKafkaBolt(), config.getEventResponseHints());
        kafkaEventSuccessProducer.addGrouping(new ShuffleGrouping(EVENT_GENERATOR));
        // -------------------------------------------------------

        // -------------------------------------------------------
        // Topology Error Handling
        // -------------------------------------------------------
        GBolt<?> errorHandlingBolt = new GRichBolt(ERROR_HANDLING, new EventErrorBolt(), config.getActionBoltHints());
        errorHandlingBolt.addGrouping(new ShuffleGrouping(KAFKA_EVENT_READER_COMPONENT, KafkaSpout.EVENT_ERROR_STREAM));

        GBolt<?> kafkaEventErrorProducer = new GRichBolt(ERROR_PRODUCER_COMPONENT, new KafkaBoltWrapper(config.getKafkaPublisherErrorBoltConfig(), String.class, String.class).getKafkaBolt(), config.getActionBoltHints());
        kafkaEventErrorProducer.addGrouping(new ShuffleGrouping(ERROR_HANDLING));
        // -------------------------------------------------------

        // Topology
        StormTopology topology = TopologyFactory.generateTopology(kafkaEventReader,
                Arrays.asList(
                        processBolt, getDataBolt, processedMessageBolt, eventGeneratorBolt, kafkaEventSuccessProducer,
                        errorHandlingBolt, kafkaEventErrorProducer));

        LOG.info("{} Topology created, submitting it to storm...", TOPOLOGY_NAME);

        return topology;
    }

    @Override
    public String name() {
        return TOPOLOGY_NAME;
    }
}
