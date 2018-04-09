package com.orwellg.yggdrasil.dsl.card.transactions.totalspendupdate;

import com.orwellg.umbrella.commons.beans.config.kafka.SubscriberKafkaConfiguration;
import com.orwellg.umbrella.commons.storm.topology.TopologyFactory;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.EventErrorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GRichBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.grouping.ShuffleGrouping;
import com.orwellg.umbrella.commons.storm.topology.generic.spout.GSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.orwellg.yggdrasil.dsl.card.transactions.config.TopologyConfig;
import com.orwellg.yggdrasil.dsl.card.transactions.config.TopologyConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TotalSpendUpdateDSLTopology {

    private final static Logger LOG = LogManager.getLogger(TotalSpendUpdateDSLTopology.class);

    private static final String TOPOLOGY_NAME = "yggdrasil-card-total-spend-update";
    private static final String PROPERTIES_FILE = "total-spend-update-topology.properties";
    private static final String KAFKA_EVENT_READER_FORMAT = "kafka-event-reader-%d";
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
        LOG.debug("Creating card total spend update topology");

        // Read configuration params from properties file and zookeeper
        TopologyConfig config = TopologyConfigFactory.getTopologyConfig(PROPERTIES_FILE);

        // Create spouts that read events from Kafka
        List<SubscriberKafkaConfiguration> kafkaSubscriberSpoutConfigs = config.getKafkaSubscriberSpoutConfigs();
        List<GSpout> kafkaEventReaders = new ArrayList<>();
        List<String> spoutNames = new ArrayList<>();
        if (kafkaSubscriberSpoutConfigs.size() == 0){
            throw new Exception("No Kafka subscriber spouts configured");
        }
        for (int i = 0; i < kafkaSubscriberSpoutConfigs.size(); i++) {
            SubscriberKafkaConfiguration subscriberConfig = kafkaSubscriberSpoutConfigs.get(i);
            String spoutName = String.format(KAFKA_EVENT_READER_FORMAT, i);
            GSpout kafkaEventReader = new GSpout(spoutName, new KafkaSpoutWrapper(subscriberConfig, String.class, String.class).getKafkaSpout(), config.getKafkaSpoutHints());
            kafkaEventReaders.add(kafkaEventReader);
            spoutNames.add(spoutName);
        }

        // Parse the events and we send it to the rest of the topology
        GBolt<?> kafkaEventProcess = new GRichBolt(KAFKA_EVENT_SUCCESS_PROCESS, new ResponseEventProcessBolt(), config.getEventProcessHints());
        for (String spoutName : spoutNames) {
            kafkaEventProcess.addGrouping(new ShuffleGrouping(spoutName, KafkaSpout.EVENT_SUCCESS_STREAM));
        }

        // Read last total spend amounts from DB
        GBolt<?> readDataBolt = new GRichBolt(READ_DATA, new LoadDataBolt(READ_DATA, PROPERTIES_FILE), config.getActionBoltHints());
        readDataBolt.addGrouping(new ShuffleGrouping(KAFKA_EVENT_SUCCESS_PROCESS));

        // Recalculate total spend amounts
        GBolt<?> recalculateBolt = new GRichBolt(RECALCULATE_SPEND_AMOUNTS, new RecalculateTotalSpendAmountsBolt(), config.getActionBoltHints());
        recalculateBolt.addGrouping(new ShuffleGrouping(READ_DATA));

        // Save new total spend amounts
        GBolt<?> saveBolt = new GRichBolt(SAVE_SPEND_AMOUNTS, new SaveTotalSpendAmountsBolt(PROPERTIES_FILE), config.getActionBoltHints());
        saveBolt.addGrouping(new ShuffleGrouping(RECALCULATE_SPEND_AMOUNTS));

        //
        // Error stream
        //

        // GBolt for work with the errors
        GBolt<?> kafkaEventError = new GRichBolt(KAFKA_EVENT_ERROR_PROCESS, new EventErrorBolt(), config.getEventErrorHints());
        for (String spoutName : spoutNames) {
            kafkaEventError.addGrouping(new ShuffleGrouping(spoutName, KafkaSpout.EVENT_ERROR_STREAM));
        }

        // GBolt for send errors of events to kafka
        GBolt<?> kafkaErrorProducer = new GRichBolt("kafka-error-producer", new KafkaBoltWrapper(config.getKafkaPublisherErrorBoltConfig(), String.class, String.class).getKafkaBolt(), config.getEventErrorHints());
        kafkaErrorProducer.addGrouping(new ShuffleGrouping(KAFKA_EVENT_ERROR_PROCESS));

        // Build the topology
        StormTopology topology = TopologyFactory.generateTopology(
                kafkaEventReaders,
                Arrays.asList(kafkaEventProcess, kafkaEventError, kafkaErrorProducer, readDataBolt, recalculateBolt, saveBolt));
        LOG.debug("Total spend update topology created");

        // Create the basic config and upload the topology
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(150);

        if (local) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, conf, topology);

            Thread.sleep(3000000);
            cluster.shutdown();
        } else {
            StormSubmitter.submitTopology(TOPOLOGY_NAME, conf, topology);
        }
    }
}
