package com.orwellg.yggdrasil.dsl.card.transactions.totalSpendUpdate;

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

public class TotalSpendUpdateDSLTopology {

    private final static Logger LOG = LogManager.getLogger(TotalSpendUpdateDSLTopology.class);

    private static final String TOPOLOGY_NAME = "card-total-spend-update-dsl";
    private static final String PROPERTIES_FILE = "total-spend-update-topology.properties";
    private static final String KAFKA_EVENT_READER = "kafka-event-reader";
    private static final String KAFKA_EVENT_SUCCESS_PROCESS = "kafka-event-success-process";
    private static final String KAFKA_EVENT_ERROR_PROCESS = "kafka-event-error-process";
    private static final String READ_DATA = "read-data";
    private static final String RECALCULATE_SPEND_AMOUNTS = "recalculate-spend-amounts";
    private static final String SAVE_SPEND_AMOUNTS = "save-spend-amounts";

    public static void main(String[] args) throws Exception {

        boolean local = false;
        if (args.length >= 1 && args[0].equals("local")) {
            local = true;
        }

        loadTopologyInStorm(local);
    }

    private static void loadTopologyInStorm(boolean local) throws Exception {
        LOG.info("Creating card total spend update topology");

        // Read configuration params from properties file and zookeeper
        TopologyConfig config = TopologyConfigFactory.getTopologyConfig(PROPERTIES_FILE);

        // Create the spout that reads events from Kafka
        GSpout kafkaEventReader = new GSpout(KAFKA_EVENT_READER, new KafkaSpoutWrapper(config.getKafkaSubscriberSpoutConfig(), String.class, String.class).getKafkaSpout(), config.getKafkaSpoutHints());

        // Parse the events and we send it to the rest of the topology
        GBolt<?> kafkaEventProcess = new GRichBolt(KAFKA_EVENT_SUCCESS_PROCESS, new ResponseEventProcessBolt(), config.getEventProcessHints());
        kafkaEventProcess.addGrouping(new ShuffleGrouping(KAFKA_EVENT_READER, KafkaSpout.EVENT_SUCCESS_STREAM));

        // Read last total spend amounts from DB
        GBolt<?> readDataBolt = new GRichBolt(READ_DATA, new ReadLastSpendingTotalsBolt(), config.getActionBoltHints());
        readDataBolt.addGrouping(new ShuffleGrouping(KAFKA_EVENT_SUCCESS_PROCESS));

        // Recalculate total spend amounts
        GBolt<?> recalculateBolt = new GRichBolt(RECALCULATE_SPEND_AMOUNTS, new RecalculateTotalSpendAmountsBolt(), config.getActionBoltHints());
        recalculateBolt.addGrouping(new ShuffleGrouping(READ_DATA));

        // Save new total spend amounts
        GBolt<?> saveBolt = new GRichBolt(SAVE_SPEND_AMOUNTS, new SaveTotalSpendAmountsBolt(), config.getActionBoltHints());
        saveBolt.addGrouping(new ShuffleGrouping(RECALCULATE_SPEND_AMOUNTS));

        //
        // Error stream
        //

        // GBolt for work with the errors
        GBolt<?> kafkaEventError = new GRichBolt(KAFKA_EVENT_ERROR_PROCESS, new EventErrorBolt(), config.getEventErrorHints());
        kafkaEventError.addGrouping(new ShuffleGrouping(KAFKA_EVENT_READER, KafkaSpout.EVENT_ERROR_STREAM));

        // GBolt for send errors of events to kafka
        GBolt<?> kafkaErrorProducer = new GRichBolt("kafka-error-producer", new KafkaBoltWrapper(config.getKafkaPublisherErrorBoltConfig(), String.class, String.class).getKafkaBolt(), config.getEventErrorHints());
        kafkaErrorProducer.addGrouping(new ShuffleGrouping(KAFKA_EVENT_ERROR_PROCESS));

        // Build the topology
        StormTopology topology = TopologyFactory.generateTopology(
                kafkaEventReader,
                Arrays.asList(kafkaEventProcess, kafkaEventError, kafkaErrorProducer, readDataBolt, recalculateBolt, saveBolt));
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
