package com.orwellg.yggdrasil.dsl.card.transactions.saveToScylla;

import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.cassandra.bolt.CassandraWriterBolt;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;


import static org.apache.storm.cassandra.DynamicStatementBuilder.async;
import static org.apache.storm.cassandra.DynamicStatementBuilder.fields;
import static org.apache.storm.cassandra.DynamicStatementBuilder.simpleQuery;

public class CardSaveScyllaGpsMessageProcessedTopology {

    private final static Logger LOG = LogManager.getLogger(CardSaveScyllaGpsMessageProcessedTopology.class);

    public static void main(String[] args) throws Exception {


        LOG.debug("Creating Card Presentments processing topology");

        TopologyBuilder builder = new TopologyBuilder();

        Integer hints = 1;

        //todo: configuration for hints
        //todo: tidy up kafka topics
        builder.setSpout("presentment-event-reader",
                new KafkaSpoutWrapper("subscriber-card-save-gps-presentment-processed.yaml", String.class, String.class).getKafkaSpout(), hints);
        builder.setSpout("authorisation-event-reader",
                new KafkaSpoutWrapper("subscriber-card-save-gps-authorisation-processed.yaml", String.class, String.class).getKafkaSpout(), hints);

        builder.setBolt("prepare-data-for-scylla",
                new CardSaveGpsMessageProcessedBolt(),
                hints
        ).shuffleGrouping("presentment-event-reader", KafkaSpout.EVENT_SUCCESS_STREAM)
         .shuffleGrouping("authorisation-event-reader", KafkaSpout.EVENT_SUCCESS_STREAM);

        //todo: add error stream


        //save message to scylla db
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

        builder.setBolt("transaction-log-scylla-insert",
                scyllaCardTransactionInsert,
                hints
        ).shuffleGrouping("prepare-data-for-scylla");

        StormTopology topology = builder.createTopology();
        LOG.info("ScyllaTransactionLogs Topology created");

        // Create the basic config and upload the topology
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(30);
        String keyspace = ComponentFactory.getConfigurationParams().getScyllaConfig().getScyllaParams().getKeyspace();
        String hostList = ComponentFactory.getConfigurationParams().getScyllaConfig().getScyllaParams().getHostList();
        //todo: add params in zookeeper for cassandra bolt ?
        conf.put("cassandra.nodes", hostList);
        conf.put("cassandra.keyspace", keyspace);
        conf.put("cassandra.port", 9042);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("card-save-scylla-gps-message-processed", conf, topology);

        Thread.sleep(3000000);
        cluster.shutdown();
    }


}
