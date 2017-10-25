package com.yggdrasil.dsl.card.transactions.topology;

import com.orwellg.umbrella.commons.storm.topology.TopologyFactory;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.EventErrorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GRichBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.grouping.ShuffleGrouping;
import com.orwellg.umbrella.commons.storm.topology.generic.spout.GSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltFieldNameWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.yggdrasil.dsl.card.transactions.topology.bolts.event.KafkaEventProcessBolt;
import com.yggdrasil.dsl.card.transactions.topology.bolts.event.PresentmentOfflineMockBolt;
import com.yggdrasil.dsl.card.transactions.topology.bolts.event.PresentmentValidateAuthorisationBolt;
import com.yggdrasil.dsl.card.transactions.topology.bolts.event.PresentmentScyllaCardTransactionsBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;

import java.util.Arrays;

public class CardPresentmentDSLTopology {

    private final static Logger LOG = LogManager.getLogger(CardPresentmentDSLTopology.class);
  
    public final static String OFFLINE_PRESENTMENT_STREAM = "offline-presentment-stream";

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
        GBolt<?> cardAuthorisationBolt = new GRichBolt("process-get-authentication", new PresentmentScyllaCardTransactionsBolt(), hints);
        cardAuthorisationBolt.addGrouping(new ShuffleGrouping("kafka-event-success-process"));


        //see if data is complete - if it is send message to kafka
        GBolt<?> authValidationBolt = new GRichBolt("process-validate-authentication", new PresentmentValidateAuthorisationBolt(), hints);
        authValidationBolt.addGrouping(new ShuffleGrouping("process-get-authentication"));


        GBolt<?> offlinePresentmentBolt = new GRichBolt("process-offline-presentment", new PresentmentOfflineMockBolt(), hints);
        offlinePresentmentBolt.addGrouping(new ShuffleGrouping("process-validate-authentication", OFFLINE_PRESENTMENT_STREAM));

        // Send a event with the result
        GBolt<?> kafkaEventSuccessProducer = new GRichBolt("kafka-event-success-producer", new KafkaBoltFieldNameWrapper("publisher-gps-dsl-presentment-msg-processed-success.yaml", String.class, String.class).getKafkaBolt(), 10);
        kafkaEventSuccessProducer.addGrouping(new ShuffleGrouping("process-validate-authentication"));
        kafkaEventSuccessProducer.addGrouping(new ShuffleGrouping("process-offline-presentment"));


        // Build the topology
        StormTopology topology = TopologyFactory.generateTopology(
                kafkaEventReader,
                Arrays.asList(kafkaEventProcess, kafkaEventError, kafkaErrorProducer, cardAuthorisationBolt, authValidationBolt, offlinePresentmentBolt, kafkaEventSuccessProducer));
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
