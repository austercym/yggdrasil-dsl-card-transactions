package com.orwellg.yggdrasil.dsl.card.transactions.earmarking;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.commons.beans.config.kafka.SubscriberKafkaConfiguration;
import com.orwellg.umbrella.commons.storm.topology.TopologyFactory;
import com.orwellg.umbrella.commons.storm.topology.component.base.AbstractTopology;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.EventErrorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaCommandGeneratorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaEventGeneratorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GRichBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.grouping.ShuffleGrouping;
import com.orwellg.umbrella.commons.storm.topology.generic.spout.GSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltFieldNameWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.orwellg.yggdrasil.card.transaction.commons.bolts.GenericEventProcessBolt;
import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfig;
import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfigFactory;
import com.orwellg.yggdrasil.dsl.card.transactions.earmarking.bolts.EarmarkingCommandBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.generated.StormTopology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EarmarkingTopology extends AbstractTopology {

    private static final Logger LOG = LogManager.getLogger(EarmarkingTopology.class);

    public static final String PROPERTIES_FILE = "earmarking-topology.properties";
    public static final String NO_EARMARKING_STREAM = "no-earmarking";
    private static final String TOPOLOGY_NAME = "yggdrasil-card-earmarking";
    private static final String BOLT_NAME_PREFIX = "earmarking";
    private static final String KAFKA_EVENT_READER_FORMAT = BOLT_NAME_PREFIX + "Reader%d";
    private static final String PROCESS_COMPONENT = BOLT_NAME_PREFIX + "Process";
    private static final String COMMAND_GENERATOR = BOLT_NAME_PREFIX + "CommandGenerator";
    private static final String ERROR_HANDLING = BOLT_NAME_PREFIX + "ErrorHandling";
    private static final String ERROR_PRODUCER_COMPONENT = BOLT_NAME_PREFIX + "ErrorProducer";
    private static final String ACCOUNTING_KAFKA_COMMAND_COMPONENT = BOLT_NAME_PREFIX + "KafkaCommandComponent";
    private static final String ACCOUNTING_PUBLISH_COMMAND_COMPONENT = BOLT_NAME_PREFIX + "PublishComponent";
    private static final String EVENT_GENERATOR = BOLT_NAME_PREFIX + "EventGenerator";
    private static final String KAFKA_EVENT_SUCCESS_PRODUCER = BOLT_NAME_PREFIX + "KafkaProducer";

    @Override
    public StormTopology load() {
        return load(null);
    }

    @Override
    public StormTopology load(String s) {
        // Read configuration params from the topology properties file and zookeeper
        TopologyConfig config = TopologyConfigFactory.getTopologyConfig(PROPERTIES_FILE);

        // -------------------------------------------------------
        // Create the spout that read events from Kafka
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
        GBolt<?> processBolt = new GRichBolt(PROCESS_COMPONENT, new GenericEventProcessBolt<>(MessageProcessed.class), config.getActionBoltHints());
        for (String spoutName : spoutNames) {
            processBolt.addGrouping(new ShuffleGrouping(spoutName, KafkaSpout.EVENT_SUCCESS_STREAM));
        }

        GBolt<?> commandGeneratorBolt = new GRichBolt(COMMAND_GENERATOR, new EarmarkingCommandBolt(), config.getActionBoltHints());
        commandGeneratorBolt.addGrouping(new ShuffleGrouping(PROCESS_COMPONENT));

        GBolt<?> eventAccountingCommandGeneratorBolt = new GRichBolt(ACCOUNTING_KAFKA_COMMAND_COMPONENT, new KafkaCommandGeneratorBolt(), config.getKafkaSpoutHints());
        eventAccountingCommandGeneratorBolt.addGrouping(new ShuffleGrouping(COMMAND_GENERATOR));

        GBolt<?> kafkaAccountingCommandProducerBolt = new GRichBolt(ACCOUNTING_PUBLISH_COMMAND_COMPONENT, new KafkaBoltFieldNameWrapper(config.getKafkaPublisherBoltConfig(), String.class, String.class).getKafkaBolt(), config.getActionBoltHints());
        kafkaAccountingCommandProducerBolt.addGrouping(new ShuffleGrouping(ACCOUNTING_KAFKA_COMMAND_COMPONENT));

        // No earmark required stream
        GBolt<?> eventGeneratorBolt = new GRichBolt(EVENT_GENERATOR, new KafkaEventGeneratorBolt(), config.getActionBoltHints());
        eventGeneratorBolt.addGrouping(new ShuffleGrouping(COMMAND_GENERATOR, NO_EARMARKING_STREAM));

        // Send a event with the result
        GBolt<?> kafkaEventSuccessProducer = new GRichBolt(KAFKA_EVENT_SUCCESS_PRODUCER, new KafkaBoltWrapper(config.getKafkaPublisherBoltConfig(), String.class, String.class).getKafkaBolt(), config.getEventResponseHints());
        kafkaEventSuccessProducer.addGrouping(new ShuffleGrouping(EVENT_GENERATOR));


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
                Arrays.asList(processBolt, commandGeneratorBolt, eventAccountingCommandGeneratorBolt, kafkaAccountingCommandProducerBolt, errorHandlingBolt, kafkaEventErrorProducer, eventGeneratorBolt, kafkaEventSuccessProducer));

        LOG.info("{} Topology created, submitting it to storm...", TOPOLOGY_NAME);

        return topology;
    }

    @Override
    public String name() {
        return TOPOLOGY_NAME;
    }
}
