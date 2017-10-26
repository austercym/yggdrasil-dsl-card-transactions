package com.yggdrasil.dsl.card.transactions.topology;

import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.yggdrasil.dsl.card.transactions.topology.bolts.event.CardSaveGpsMessageProcessedBolt;
import com.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
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

public class CardSaveGpsMessageProcessed {

    private final static Logger LOG = LogManager.getLogger(CardSaveGpsMessageProcessed.class);

    public static void main(String[] args) throws Exception {


        LOG.debug("Creating Card Presentments processing topology");

        TopologyBuilder builder = new TopologyBuilder();

        Integer hints = 1;

        //todo: configuration for hints
        //todo: tidy up kafka topics
        builder.setSpout("transaction-log-event-reader",
                new KafkaSpoutWrapper("subscriber-card-save-gps-presentment-processed.yaml", String.class, String.class).getKafkaSpout(), hints);

        builder.setBolt("prepare-data-for-scylla",
                new CardSaveGpsMessageProcessedBolt(),
                hints
        ).shuffleGrouping("transaction-log-event-reader", KafkaSpout.EVENT_SUCCESS_STREAM);

        //save message to scylla db
        CassandraWriterBolt scyllaCardTransactionInsert = new CassandraWriterBolt(
                async(
                        simpleQuery("INSERT INTO CardTransactions (GpsTransactionLink, GpsTransactionId, DebitCardId, TransactionTimestamp, InternalAccountId, WirecardAmount, WirecardCurrency, BlockedClientAmount, BlockedClientCurrency) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);")
                                .with(
                                        fields("gpsTransactionLink", "gpsTransactionId", "debitCardId", "transactionTimestamp", "internalAccountId", "wirecardAmount", "wirecardCurrency", "blockedClientAmount", "blockedClientCurrency")
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
        String nodeList = ComponentFactory.getConfigurationParams().getScyllaConfig().getScyllaParams().getNodeList();
        conf.put("cassandra.nodes", "localhost");
        conf.put("cassandra.keyspace", keyspace);
        conf.put("cassandra.port", 9042);
        //conf.setNumWorkers(ScyllaTransactionLogDSLConfigFactory.getScyllaTransactionDSLConfig().getTopologyNumWorkers());
        //conf.setMaxTaskParallelism(ScyllaTransactionLogDSLConfigFactory.getScyllaTransactionDSLConfig().getTopologyMaxTaskParallelism())

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("dsl-gps", conf, topology);

        Thread.sleep(3000000);
        cluster.shutdown();
    }


}
