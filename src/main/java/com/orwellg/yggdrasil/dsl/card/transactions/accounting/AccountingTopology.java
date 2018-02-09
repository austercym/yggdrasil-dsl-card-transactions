package com.orwellg.yggdrasil.dsl.card.transactions.accounting;

import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.storm.topology.TopologyFactory;
import com.orwellg.umbrella.commons.storm.topology.component.base.AbstractTopology;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.EventErrorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GRichBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.grouping.ShuffleGrouping;
import com.orwellg.umbrella.commons.storm.topology.generic.spout.GSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.orwellg.yggdrasil.dsl.card.transactions.accounting.bolts.ProcessBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.generated.StormTopology;

import java.util.Arrays;

public class AccountingTopology extends AbstractTopology {

    private static final Logger LOG = LogManager.getLogger(AccountingTopology.class);

    public static final String PROPERTIES_FILE = "accounting-topology.properties.properties";
    private static final String TOPOLOGY_NAME = "dsl-cards-accounting";
    private static final String BOLT_NAME_PREFIX = "accounting";
    private static final String KAFKA_EVENT_READER_COMPONENT = BOLT_NAME_PREFIX + "Reader";
    private static final String PROCESS_COMPONENT = BOLT_NAME_PREFIX + "Process";
    private static final String ERROR_HANDLING = BOLT_NAME_PREFIX + "ErrorHandling";
    private static final String ERROR_PRODUCER_COMPONENT = BOLT_NAME_PREFIX + "ErrorProducer";

    @Override
    public StormTopology load() {
        // Read configuration params from the topology properties file and zookeeper
        TopologyConfig config = TopologyConfigFactory.getTopologyConfig(PROPERTIES_FILE);

        // -------------------------------------------------------
        // Create the spout that read events from Kafka
        // -------------------------------------------------------
        GSpout kafkaEventReader = new GSpout(KAFKA_EVENT_READER_COMPONENT,
                new KafkaSpoutWrapper(config.getKafkaSubscriberSpoutConfig(), String.class, String.class).getKafkaSpout(),
                config.getKafkaSpoutHints());

        // -------------------------------------------------------
        // Process events
        // -------------------------------------------------------
        GBolt<?> processBolt = new GRichBolt(PROCESS_COMPONENT, new ProcessBolt(),
                config.getActionBoltHints());
        processBolt.addGrouping(new ShuffleGrouping(KAFKA_EVENT_READER_COMPONENT, KafkaSpout.EVENT_SUCCESS_STREAM));
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
                Arrays.asList(processBolt, errorHandlingBolt, kafkaEventErrorProducer));

        LOG.info("{} Topology created, submitting it to storm...", TOPOLOGY_NAME);

        return topology;
    }

    @Override
    public String name() {
        return TOPOLOGY_NAME;
    }
}
