package com.orwellg.yggdrasil.dsl.card.transactions.saveToScylla;

import com.orwellg.umbrella.commons.beans.config.kafka.SubscriberKafkaConfiguration;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.EventErrorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.topology.generic.grouping.ShuffleGrouping;
import com.orwellg.umbrella.commons.storm.topology.generic.spout.GSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.orwellg.yggdrasil.dsl.card.transactions.config.TopologyConfig;
import com.orwellg.yggdrasil.dsl.card.transactions.config.TopologyConfigFactory;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.cassandra.bolt.CassandraWriterBolt;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.BoltDeclarer;
import org.apache.storm.topology.TopologyBuilder;

import java.util.ArrayList;
import java.util.List;

import static org.apache.storm.cassandra.DynamicStatementBuilder.async;
import static org.apache.storm.cassandra.DynamicStatementBuilder.fields;
import static org.apache.storm.cassandra.DynamicStatementBuilder.simpleQuery;



public class CardSaveScyllaGpsMessageProcessedTopology {

    private final static Logger LOG = LogManager.getLogger(CardSaveScyllaGpsMessageProcessedTopology.class);
    public static final String TOPOLOGY_NAME = "dsl-card-transactions-scylla";
    public static final String PRESENTMENT_SPOUT_NAME = "presentment-event-reader";
    public static final String AUTHORISATION_SPOUT_NAME = "authorisation-event-reader";
    public static final String SCYLLA_PREPARE_NAME = "prepare-data-for-scylla";
    public static final String SCYLLA_SAVE_NAME = "save-to-scylla";
    public static final String SCYLLA_ERROR_HANDLER_NAME = "error-handler";
    public static final String SCYLLA_ERROR_PUBLISHER_NAME = "error-publisher";

    private static final String KAFKA_EVENT_READER_FORMAT = "kafka-event-reader-%d";

    public static void main(String[] args) throws Exception {

        boolean isLocal = false;
        if (args.length >= 1 && args[0].equals("local")) {
            isLocal = true;
        }
        loadTopologyInStorm(isLocal);
    }


    public static void loadTopologyInStorm(Boolean isLocal) throws Exception {

        LOG.debug("Creating Card Presentments processing topology");

        TopologyBuilder builder = new TopologyBuilder();
        TopologyConfig config = TopologyConfigFactory.getTopologyConfig("scylla-topology.properties");
        List<SubscriberKafkaConfiguration> kafkaSubscriberSpoutConfigs = config.getKafkaSubscriberSpoutConfigs();
        Integer scyllaHints = config.getEventProcessHints();
        Integer errorHints = config.getEventErrorHints();

        //------------------- Read from multiple kafka streams --------------------

        List<String> spoutNames = new ArrayList<>();
        for (int i = 0; i < config.getKafkaSubscriberSpoutConfigs().size(); i++) {
            SubscriberKafkaConfiguration subscriberConfig = kafkaSubscriberSpoutConfigs.get(i);
            String spoutName = String.format(KAFKA_EVENT_READER_FORMAT, i);
            spoutNames.add(spoutName);
            builder.setSpout(spoutName, new KafkaSpoutWrapper(subscriberConfig, String.class, String.class).getKafkaSpout(), config.getKafkaSpoutHints());
        }

        //------------------- Parse event and send fields forward -------------------


        BoltDeclarer saveToScyllaBoltDeclarer = builder.setBolt(SCYLLA_PREPARE_NAME,
                new CardSaveGpsMessageProcessedBolt(),
                scyllaHints
        );
        for (String spoutName : spoutNames) {
            saveToScyllaBoltDeclarer.shuffleGrouping(spoutName, KafkaSpout.EVENT_SUCCESS_STREAM);
        }


        // ------------------- Save To cards.CardTransactions ---------
        CassandraWriterBolt scyllaCardTransactionInsert = new CassandraWriterBolt(
                async(
                        simpleQuery("INSERT INTO CardTransactions (GpsTransactionLink, GpsTransactionId, GpsTransactionDateTime, DebitCardId, TransactionTimestamp, InternalAccountId, " +
                                "WirecardAmount, WirecardCurrency, BlockedClientAmount, BlockedClientCurrency, GpsMessageType, FeeAmount, FeeCurrency, InternalAccountCurrency) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")
                                .with(
                                        fields("gpsTransactionLink", "gpsTransactionId", "gpsTransactionDateTime", "debitCardId", "transactionTimestamp", "internalAccountId",
                                                "wirecardAmount", "wirecardCurrency", "blockedClientAmount", "blockedClientCurrency", "gpsMessageType", "feeAmount", "feeCurrency", "internalAccountCurrency")
                                )
                )
        );
        builder.setBolt(SCYLLA_SAVE_NAME,
                scyllaCardTransactionInsert,
                scyllaHints
        ).shuffleGrouping(SCYLLA_PREPARE_NAME);

        // ------------ Manage Errors ------------------------------

        BoltDeclarer errorHandlerDeclarer = builder.setBolt(SCYLLA_ERROR_HANDLER_NAME,
                new EventErrorBolt(),
                errorHints
        );
        for (String spoutName : spoutNames) {
            errorHandlerDeclarer.shuffleGrouping(spoutName, KafkaSpout.EVENT_SUCCESS_STREAM);
        }

        builder.setBolt(SCYLLA_ERROR_PUBLISHER_NAME,
                new KafkaBoltWrapper(config.getKafkaPublisherErrorBoltConfig(), String.class, String.class).getKafkaBolt(),
                errorHints
        ).shuffleGrouping(SCYLLA_ERROR_HANDLER_NAME);


        //---------------Build Topology -------------------------------

        StormTopology topology = builder.createTopology();
        LOG.info("ScyllaTransactionLogs Topology created");

        String keyspace = ComponentFactory.getConfigurationParams().getCardsScyllaParams().getKeyspace();
        String hostList = ComponentFactory.getConfigurationParams().getCardsScyllaParams().getHostList();
        // Create the basic config and upload the topology
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(30);
        conf.put("cassandra.keyspace", keyspace);
        conf.put("cassandra.nodes", hostList);


        if (isLocal) {
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
