package com.orwellg.yggdrasil.dsl.card.transactions.presentment;

import com.orwellg.umbrella.commons.beans.config.kafka.PublisherKafkaConfiguration;
import com.orwellg.umbrella.commons.beans.config.kafka.SubscriberKafkaConfiguration;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
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
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;

import java.util.Arrays;

public class CardPresentmentDSLTopology {

    private final static Logger LOG = LogManager.getLogger(CardPresentmentDSLTopology.class);
  
    public final static String OFFLINE_PRESENTMENT_STREAM = "offline-presentment-stream";
    public final static String ERROR_STREAM = "error-stream";
    public static final String TOPOLOGY_NAME = "card-presentment-dsl";


    public static void main(String[] args) throws Exception {

        boolean local = false;
        if (args.length >= 1 && args[0].equals("local")) {
            local = true;
        }

        loadTopologyInStorm(local);
    }

    private static void loadTopologyInStorm(boolean local) throws Exception {
        LOG.debug("Creating Card Presentments processing topology");

        TopologyConfig topologyConfig = TopologyConfigFactory.getTopologyConfig("presentment.topology.properties");
        int hintsSpout = topologyConfig.getKafkaSpoutHints();
        int hintsProcessors = topologyConfig.getActionBoltHints();

        // Create the spout that read the events from Kafka
        GSpout kafkaEventReader = new GSpout("kafka-event-reader",
                new KafkaSpoutWrapper( topologyConfig.getKafkaSubscriberSpoutConfig(), String.class, String.class).getKafkaSpout(), hintsSpout);

        // Parse the events and we send it to the rest of the topology
        GBolt<?> kafkaEventProcess = new GRichBolt("process-kafka-message", new ProcessKafkaMessage(), hintsProcessors);
        kafkaEventProcess.addGrouping(new ShuffleGrouping("kafka-event-reader", KafkaSpout.EVENT_SUCCESS_STREAM));


        //------------------------- Processing PresentmentMessage --------------------------------------------------------------

        //Get card authorisation data
        GBolt<?> cardAuthorisationBolt = new GRichBolt("get-card-transactions", new GetCardTransactions(), hintsProcessors);
        cardAuthorisationBolt.addGrouping(new ShuffleGrouping("process-kafka-message"));

        //see if this is offline presentment
        GBolt<?> authValidationBolt = new GRichBolt("process-card-transactions", new ProcessCardTransaction(), hintsProcessors);
        authValidationBolt.addGrouping(new ShuffleGrouping("get-card-transactions"));

        //------------------------ Offline PresentmentMessage Processing -------------------------------------------------------

        //offline presentment - needs linked account in time of transaction
        GBolt<?> getLinkedAccountBolt = new GRichBolt("get-linked-account", new GetLinkedAccount(), hintsProcessors);
        getLinkedAccountBolt.addGrouping(new ShuffleGrouping("process-card-transactions", OFFLINE_PRESENTMENT_STREAM));

        GBolt<?> validateLinikedAccountBolt = new GRichBolt("process-linked-account", new ProcessOfflineTransaction(), hintsProcessors);
        validateLinikedAccountBolt.addGrouping(new ShuffleGrouping("get-linked-account"));

        //------------------------- Calculating Fees, Wirecard Amounts, Client Amounts -------------------------------

        GBolt<?> getFeeSchemaBolt = new GRichBolt("get-fees-schema", new GetFeeSchema(), hintsProcessors);
        getFeeSchemaBolt.addGrouping(new ShuffleGrouping("process-linked-account"));
        getFeeSchemaBolt.addGrouping(new ShuffleGrouping("process-card-transactions"));

        //calculate client, wirecard, fees amounts
        GBolt<?> calculateAmountsBolt = new GRichBolt("process-fee-schema", new ProcessFeeSchema(), hintsProcessors);
        calculateAmountsBolt.addGrouping(new ShuffleGrouping("get-fees-schema"));

        //------------------------- Send an event with the result -------------------------------------------------------
        //GBolt<?> kafkaEventSuccessProducer = new GRichBolt("kafka-event-success-producer", new KafkaBoltFieldNameWrapper(topologyConfig.getKafkaPublisherBoltConfig(), String.class, String.class).getKafkaBolt(), 10);
        GBolt<?> kafkaEventSuccessProducer = new GRichBolt("kafka-success-producer", new KafkaBoltWrapper(topologyConfig.getKafkaPublisherBoltConfig(), String.class, String.class).getKafkaBolt(), topologyConfig.getEventResponseHints());
        kafkaEventSuccessProducer.addGrouping(new ShuffleGrouping("process-fee-schema"));

        //-------------------------------- Error Handling --------------------------------------------------------------
        // GBolt for work with the errors
        GBolt<?> kafkaEventError = new GRichBolt("kafka-event-error-process", new EventErrorBolt(), topologyConfig.getEventErrorHints());
        kafkaEventError.addGrouping(new ShuffleGrouping("kafka-event-reader", KafkaSpout.EVENT_ERROR_STREAM));


        GBolt<?> gpsErrorBolt = new GRichBolt("gps-error-handler", new ProcessExceptionBolt(), topologyConfig.getEventErrorHints());
        gpsErrorBolt.addGrouping(new ShuffleGrouping("process-kafka-message", ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping("get-card-transactions", ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping("process-card-transactions", ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping("get-linked-account", ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping("process-linked-account", ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping("get-fees-schema", ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping("process-fee-schema", ERROR_STREAM));


        // GBolt for send errors of events to kafka
        GBolt<?> kafkaErrorProducer = new GRichBolt("kafka-error-producer", new KafkaBoltWrapper(topologyConfig.getKafkaPublisherErrorBoltConfig(), String.class, String.class).getKafkaBolt(), topologyConfig.getEventErrorHints());
        kafkaErrorProducer.addGrouping(new ShuffleGrouping("kafka-event-error-process"));
        kafkaErrorProducer.addGrouping(new ShuffleGrouping("gps-error-handler"));


        // Build the topology
        StormTopology topology = TopologyFactory.generateTopology(
                kafkaEventReader,
                Arrays.asList(kafkaEventProcess, cardAuthorisationBolt, authValidationBolt,
                        getLinkedAccountBolt, validateLinikedAccountBolt, getFeeSchemaBolt, calculateAmountsBolt, kafkaEventSuccessProducer, kafkaEventError, gpsErrorBolt, kafkaErrorProducer));
        LOG.debug("Topology created");

        // Create the basic config and upload the topology
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(30);

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
