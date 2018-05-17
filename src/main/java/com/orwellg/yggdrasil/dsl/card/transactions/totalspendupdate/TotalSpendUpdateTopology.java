package com.orwellg.yggdrasil.dsl.card.transactions.totalspendupdate;

import com.orwellg.umbrella.commons.beans.config.kafka.SubscriberKafkaConfiguration;
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
import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfig;
import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfigFactory;
import com.orwellg.yggdrasil.dsl.card.transactions.totalspendupdate.bolts.LoadDataBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.totalspendupdate.bolts.RecalculateTotalSpendAmountsBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.totalspendupdate.bolts.ResponseEventProcessBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.totalspendupdate.bolts.SaveTotalSpendAmountsBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.generated.StormTopology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TotalSpendUpdateTopology extends AbstractTopology {

    public static final String PROPERTIES_FILE = "total-spend-update-topology.properties";
    private static final Logger LOG = LogManager.getLogger(TotalSpendUpdateTopology.class);
    private static final String TOPOLOGY_NAME = "yggdrasil-card-total-spend-update";
    private static final String BOLT_NAME_PREFIX = "totalSpendUpdate";
    private static final String KAFKA_EVENT_READER_FORMAT = BOLT_NAME_PREFIX + "Reader%d";
    private static final String PROCESS_COMPONENT = BOLT_NAME_PREFIX + "Process";
    private static final String ERROR_HANDLING = BOLT_NAME_PREFIX + "ErrorHandling";
    private static final String ERROR_PRODUCER_COMPONENT = BOLT_NAME_PREFIX + "ErrorProducer";
    private static final String READ_DATA = BOLT_NAME_PREFIX + "ReadData";
    private static final String RECALCULATE_SPEND_AMOUNTS = BOLT_NAME_PREFIX + "RecalculateSpendAmounts";
    private static final String SAVE_SPEND_AMOUNTS = BOLT_NAME_PREFIX + "SaveSpendAmounts";
    private static final String EVENT_GENERATOR = BOLT_NAME_PREFIX + "EventGenerator";
    private static final String KAFKA_EVENT_SUCCESS_PRODUCER = BOLT_NAME_PREFIX + "KafkaProducer";

    @Override
    public StormTopology load() {
        // Read configuration params from the topology properties file and zookeeper
        TopologyConfig config = TopologyConfigFactory.getTopologyConfig(PROPERTIES_FILE);

        // -------------------------------------------------------
        // Create spouts that read events from Kafka
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
        GBolt<?> kafkaEventProcess = new GRichBolt(PROCESS_COMPONENT, new ResponseEventProcessBolt(), config.getEventProcessHints());
        for (String spoutName : spoutNames) {
            kafkaEventProcess.addGrouping(new ShuffleGrouping(spoutName, KafkaSpout.EVENT_SUCCESS_STREAM));
        }

        // Read last total spend amounts from DB
        GBolt<?> readDataBolt = new GRichBolt(READ_DATA, new LoadDataBolt(READ_DATA, PROPERTIES_FILE), config.getActionBoltHints());
        readDataBolt.addGrouping(new ShuffleGrouping(PROCESS_COMPONENT));

        // Recalculate total spend amounts
        GBolt<?> recalculateBolt = new GRichBolt(RECALCULATE_SPEND_AMOUNTS, new RecalculateTotalSpendAmountsBolt(), config.getActionBoltHints());
        recalculateBolt.addGrouping(new ShuffleGrouping(READ_DATA));

        // Save new total spend amounts
        GBolt<?> saveBolt = new GRichBolt(SAVE_SPEND_AMOUNTS, new SaveTotalSpendAmountsBolt(PROPERTIES_FILE), config.getActionBoltHints());
        saveBolt.addGrouping(new ShuffleGrouping(RECALCULATE_SPEND_AMOUNTS));

        GBolt<?> eventGeneratorBolt = new GRichBolt(EVENT_GENERATOR, new KafkaEventGeneratorBolt(), config.getActionBoltHints());
        eventGeneratorBolt.addGrouping(new ShuffleGrouping(SAVE_SPEND_AMOUNTS));

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
                Arrays.asList(kafkaEventProcess, readDataBolt, recalculateBolt, saveBolt, eventGeneratorBolt, kafkaEventSuccessProducer, errorHandlingBolt, kafkaEventErrorProducer));

        LOG.info("{} Topology created, submitting it to storm...", TOPOLOGY_NAME);

        return topology;
    }

    @Override
    public String name() {
        return TOPOLOGY_NAME;
    }
}
