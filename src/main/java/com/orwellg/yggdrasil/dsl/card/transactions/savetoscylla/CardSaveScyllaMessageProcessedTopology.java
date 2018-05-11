package com.orwellg.yggdrasil.dsl.card.transactions.savetoscylla;

import com.orwellg.umbrella.commons.beans.config.kafka.SubscriberKafkaConfiguration;
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
import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfig;
import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfigFactory;
import com.orwellg.yggdrasil.dsl.card.transactions.savetoscylla.bolts.PrepareDataBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.savetoscylla.bolts.SaveBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.generated.StormTopology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CardSaveScyllaMessageProcessedTopology extends AbstractTopology {

    private static final Logger LOG = LogManager.getLogger(CardSaveScyllaMessageProcessedTopology.class);

    public static final String PROPERTIES_FILE = "scylla-topology.properties";
    private static final String TOPOLOGY_NAME = "yggdrasil-card-message-processed-scylla";
    private static final String BOLT_NAME_PREFIX = "messageProcessedScylla";
    private static final String KAFKA_EVENT_READER_FORMAT = BOLT_NAME_PREFIX + "Reader%d";
    private static final String PROCESS_COMPONENT = BOLT_NAME_PREFIX + "Process";
    private static final String SAVE_COMPONENT = BOLT_NAME_PREFIX + "Save";
    private static final String ERROR_HANDLING = BOLT_NAME_PREFIX + "ErrorHandling";
    private static final String ERROR_PRODUCER_COMPONENT = BOLT_NAME_PREFIX + "ErrorProducer";

    @Override
    public StormTopology load() {
        // Read configuration params from the topology properties file and zookeeper
        TopologyConfig config = TopologyConfigFactory.getTopologyConfig(PROPERTIES_FILE);

        // -------------------------------------------------------
        // Create the spouts that read events from Kafka
        // -------------------------------------------------------
        List<SubscriberKafkaConfiguration> kafkaSubscriberSpoutConfigs = config.getKafkaSubscriberSpoutConfigs();
        List<GSpout> kafkaEventReaders = new ArrayList<>();
        List<String> spoutNames = new ArrayList<>();
        for (int i = 0; i < kafkaSubscriberSpoutConfigs.size(); i++) {
            SubscriberKafkaConfiguration subscriberConfig = kafkaSubscriberSpoutConfigs.get(i);
            String spoutName = String.format(KAFKA_EVENT_READER_FORMAT, i);
            GSpout kafkaEventReader = new GSpout(spoutName, new KafkaSpoutWrapper(subscriberConfig, String.class, String.class).getKafkaSpout(), config.getKafkaSpoutHints());
            kafkaEventReaders.add(kafkaEventReader);
            spoutNames.add(spoutName);
        }

        // -------------------------------------------------------
        // Process events
        // -------------------------------------------------------
        GBolt<?> processBolt = new GRichBolt(PROCESS_COMPONENT, new PrepareDataBolt(), config.getActionBoltHints());
        for (String spoutName : spoutNames) {
            processBolt.addGrouping(new ShuffleGrouping(spoutName, KafkaSpout.EVENT_SUCCESS_STREAM));
        }

        GBolt<?> saveBolt = new GRichBolt(SAVE_COMPONENT, new SaveBolt(PROPERTIES_FILE), config.getActionBoltHints());
        saveBolt.addGrouping(new ShuffleGrouping(PROCESS_COMPONENT));
        // -------------------------------------------------------

        // -------------------------------------------------------
        // Topology Error Handling
        // -------------------------------------------------------
        GBolt<?> errorHandlingBolt = new GRichBolt(ERROR_HANDLING, new EventErrorBolt(), config.getActionBoltHints());
        for (String spoutName : spoutNames) {
            errorHandlingBolt.addGrouping(new ShuffleGrouping(spoutName, KafkaSpout.EVENT_ERROR_STREAM));
        }

        GBolt<?> kafkaEventErrorProducer = new GRichBolt(ERROR_PRODUCER_COMPONENT, new KafkaBoltWrapper(config.getKafkaPublisherErrorBoltConfig(), String.class, String.class).getKafkaBolt(), config.getActionBoltHints());
        kafkaEventErrorProducer.addGrouping(new ShuffleGrouping(ERROR_HANDLING));
        // -------------------------------------------------------

        // Topology
        StormTopology topology = TopologyFactory.generateTopology(kafkaEventReaders,
                Arrays.asList(processBolt, saveBolt, errorHandlingBolt, kafkaEventErrorProducer));

        LOG.info("{} Topology created, submitting it to storm...", TOPOLOGY_NAME);

        return topology;
    }

    @Override
    public String name() {
        return TOPOLOGY_NAME;
    }
}
