package com.yggdrasil.dsl.card.transactions.topology;

import com.orwellg.umbrella.commons.storm.topology.TopologyFactory;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.EventErrorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GRichBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.grouping.ShuffleGrouping;
import com.orwellg.umbrella.commons.storm.topology.generic.spout.GSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.yggdrasil.dsl.card.transactions.topology.bolts.event.KafkaEventProcessBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;

import java.util.Arrays;

public class CardPresentmentDSLTopology {

    private final static Logger LOG = LogManager.getLogger(CardTransactionsDSLTopology.class);

    public static void main(String[] args) throws Exception {

        LOG.debug("Creating Card Presentments processing topology");

        Integer hints = 1;

        // Create the spout that read the events from Kafka
        GSpout kafkaEventReader = new GSpout("kafka-event-reader", new KafkaSpoutWrapper("subscriber-gps-presentment-dsl-topic.yaml", String.class, String.class).getKafkaSpout(), hints);

        // Parse the events and we send it to the rest of the topology
        GBolt<?> kafkaEventProcess = new GRichBolt("kafka-event-success-process", new KafkaEventProcessBolt(), hints);
        kafkaEventProcess.addGrouping(new ShuffleGrouping("kafka-event-reader", KafkaSpout.EVENT_SUCCESS_STREAM));

        // GBolt for work with the errors
        GBolt<?> kafkaEventError = new GRichBolt("kafka-event-error-process", new EventErrorBolt(), hints);
        kafkaEventError.addGrouping(new ShuffleGrouping("kafka-event-reader", KafkaSpout.EVENT_ERROR_STREAM));

        // GBolt for send errors of events to kafka
        GBolt<?> kafkaErrorProducer = new GRichBolt("kafka-error-producer", new KafkaBoltWrapper("publisher-gps-dsl-error.yaml", String.class, String.class).getKafkaBolt(), hints);
        kafkaErrorProducer.addGrouping(new ShuffleGrouping("kafka-event-error-process"));

        //1.get gpd transaction link to check if there was an auth before
        //2.get auth data from db
        //3.process data
        //4.publish result on new topic
        /*
        get msg from kafka -> get data from db
        auth    ->	 										publish complete message
        no auth ->   get the card in time of transaction -> publish complete message
        (read from scylla)

        get complete message -> create messages for accounting
        get complete message -> save to hive
        get complete message -> save to scylla
        */


        //Get card authorisation data
        GBolt<?> sampleBolt = new GRichBolt("process-db-access", new ScyllaCardTransactionsBolt(), hints);
        sampleBolt.addGrouping(new ShuffleGrouping("kafka-event-success-process"));


        //see if data is complete - if it is send message to kafka
        //if it is not - try to get card data from
        //see if this is offline transaction -> try to get account connected to the transaction in the time of the transaction
        //create avro type for sending out presentment response message



        // Build the topology
        StormTopology topology = TopologyFactory.generateTopology(
                kafkaEventReader,
                Arrays.asList(kafkaEventProcess, kafkaEventError, kafkaErrorProducer, sampleBolt));
        LOG.debug("Topology created");

        // Create the basic config and upload the topology
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(30);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("dsl-gps", conf, topology);

        Thread.sleep(3000000);
        cluster.shutdown();

    }
}
