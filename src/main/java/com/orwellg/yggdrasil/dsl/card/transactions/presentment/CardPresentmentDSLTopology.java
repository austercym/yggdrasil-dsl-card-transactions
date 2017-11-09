package com.orwellg.yggdrasil.dsl.card.transactions.presentment;

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
import com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.event.KafkaEventProcessBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;

import java.util.Arrays;

public class CardPresentmentDSLTopology {

    private final static Logger LOG = LogManager.getLogger(CardPresentmentDSLTopology.class);
  
    public final static String OFFLINE_PRESENTMENT_STREAM = "offline-presentment-stream";
    public final static String ERROR_STREAM = "error-stream";


    public static void main(String[] args) throws Exception {

        LOG.debug("Creating Card Presentments processing topology");

        Integer hints = 1;

        // Create the spout that read the events from Kafka
        GSpout kafkaEventReader = new GSpout("kafka-event-reader", new KafkaSpoutWrapper("subscriber-gps-presentment-dsl-topic.yaml", String.class, String.class).getKafkaSpout(), hints);

        // Parse the events and we send it to the rest of the topology
        GBolt<?> kafkaEventProcess = new GRichBolt("process-kafka-message", new ProcessKafkaMessage(), hints);
        kafkaEventProcess.addGrouping(new ShuffleGrouping("kafka-event-reader", KafkaSpout.EVENT_SUCCESS_STREAM));


        //------------------------- Processing PresentmentMessage --------------------------------------------------------------

        //Get card authorisation data
        GBolt<?> cardAuthorisationBolt = new GRichBolt("get-card-transactions", new GetCardTransactions(), hints);
        cardAuthorisationBolt.addGrouping(new ShuffleGrouping("process-kafka-message"));

        //see if this is offline presentment
        GBolt<?> authValidationBolt = new GRichBolt("process-card-transactions", new ProcessCardTransaction(), hints);
        authValidationBolt.addGrouping(new ShuffleGrouping("get-card-transactions"));

        //------------------------ Offline PresentmentMessage Processing -------------------------------------------------------

        //offline presentment - needs linked account in time of transaction
        GBolt<?> getLinkedAccountBolt = new GRichBolt("get-linked-account", new GetLinkedAccount(), hints);
        getLinkedAccountBolt.addGrouping(new ShuffleGrouping("process-card-transactions", OFFLINE_PRESENTMENT_STREAM));

        GBolt<?> validateLinikedAccountBolt = new GRichBolt("process-linked-account", new ProcessOfflineTransaction(), hints);
        validateLinikedAccountBolt.addGrouping(new ShuffleGrouping("get-linked-account"));

        //------------------------- Calculating Fees, Wirecard Amounts, Client Amounts -------------------------------

        GBolt<?> getFeeSchemaBolt = new GRichBolt("get-fees-schema", new GetFeeSchema(), hints);
        getFeeSchemaBolt.addGrouping(new ShuffleGrouping("process-linked-account"));
        getFeeSchemaBolt.addGrouping(new ShuffleGrouping("process-card-transactions"));

        //calculate client, wirecard, fees amounts
        GBolt<?> calculateAmountsBolt = new GRichBolt("process-fee-schema", new ProcessFeeSchema(), hints);
        calculateAmountsBolt.addGrouping(new ShuffleGrouping("get-fees-schema"));

        //------------------------- Send an event with the result -------------------------------------------------------
        GBolt<?> kafkaEventSuccessProducer = new GRichBolt("kafka-event-success-producer", new KafkaBoltFieldNameWrapper("publisher-gps-dsl-presentment-msg-processed-success.yaml", String.class, String.class).getKafkaBolt(), 10);
        kafkaEventSuccessProducer.addGrouping(new ShuffleGrouping("process-fee-schema"));

        //-------------------------------- Error Handling --------------------------------------------------------------
        // GBolt for work with the errors
        GBolt<?> kafkaEventError = new GRichBolt("kafka-event-error-process", new EventErrorBolt(), hints);
        kafkaEventError.addGrouping(new ShuffleGrouping("kafka-event-reader", KafkaSpout.EVENT_ERROR_STREAM));


        GBolt<?> gpsErrorBolt = new GRichBolt("gps-error-handler", new ProcessExceptionBolt(), hints);
        gpsErrorBolt.addGrouping(new ShuffleGrouping("process-kafka-message", ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping("get-card-transactions", ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping("process-card-transactions", ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping("get-linked-account", ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping("process-linked-account", ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping("get-fees-schema", ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping("process-fee-schema", ERROR_STREAM));


        // GBolt for send errors of events to kafka
        GBolt<?> kafkaErrorProducer = new GRichBolt("kafka-error-producer", new KafkaBoltWrapper("publisher-gps-dsl-error.yaml", String.class, String.class).getKafkaBolt(), hints);
        kafkaErrorProducer.addGrouping(new ShuffleGrouping("kafka-event-error-process"));
        kafkaErrorProducer.addGrouping(new ShuffleGrouping("gps-error-handler"));


        // Build the topology
        StormTopology topology = TopologyFactory.generateTopology(
                kafkaEventReader,
                Arrays.asList(kafkaEventProcess, cardAuthorisationBolt, authValidationBolt,
                        getLinkedAccountBolt, validateLinikedAccountBolt, getFeeSchemaBolt, calculateAmountsBolt, kafkaEventSuccessProducer, kafkaEventError, gpsErrorBolt, kafkaErrorProducer));
        //StormTopology topology = TopologyFactory.generateTopology(
        //        kafkaEventReader,
        //        Arrays.asList(kafkaEventProcess, cardAuthorisationBolt, authValidationBolt, getLinkedAccountBolt, validateLinikedAccountBolt));

        LOG.debug("Topology created");

        // Create the basic config and upload the topology
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(30);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("card-presentment-dsl", conf, topology);

        Thread.sleep(3000000);
        cluster.shutdown();

    }
}
