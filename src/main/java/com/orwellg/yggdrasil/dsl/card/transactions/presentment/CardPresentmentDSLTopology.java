package com.orwellg.yggdrasil.dsl.card.transactions.presentment;

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

public class CardPresentmentDSLTopology {

    private final static Logger LOG = LogManager.getLogger(CardPresentmentDSLTopology.class);
  
    public final static String OFFLINE_PRESENTMENT_STREAM = "offline-presentment-stream";
    public final static String ERROR_STREAM = "error-stream";
    public static final String TOPOLOGY_NAME = "dsl-card-presentment";

    private final static String BOLT_KAFKA_READER_NAME = "kafka-event-reader";
    private final static String BOLT_KAFKA_PROCESS_MESSAGE = "process-kafka-message";
    private final static String BOLT_GET_TRANSACTIONS = "get-card-transactions";
    private final static String BOLT_CHECK_AUTHORISATION = "check-authorisation";
    private final static String BOLT_GET_LINKED_ACCOUNT = "get-linked-account";
    private final static String BOLT_PROCESS_LINKED_ACCOUNT = "process-linked-account";
    private final static String BOLT_GET_FEE_SCHEMA = "get-fees-schema";
    private final static String BOLT_PROCESS_FEE_SCHEMA = "process-fee-schema";
    private final static String BOLT_KAFKA_SUCCESS_PRODUCER = "kafka-success-producer";
    private final static String BOLT_KAFKA_EVENT_ERROR = "kafka-event-error-process";
    private final static String BOLT_GPS_ERROR_HANDLER = "gps-error-handler";
    private final static String BOLT_KAFKA_ERROR_PRODUCER = "kafka-error-producer";

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
        GSpout kafkaEventReader = new GSpout(BOLT_KAFKA_READER_NAME,
                new KafkaSpoutWrapper( topologyConfig.getKafkaSubscriberSpoutConfig(), String.class, String.class).getKafkaSpout(), hintsSpout);

        // Parse the events and we send it to the rest of the topology
        GBolt<?> kafkaEventProcess = new GRichBolt(BOLT_KAFKA_PROCESS_MESSAGE, new EventToPresentmentBolt(), hintsProcessors);
        kafkaEventProcess.addGrouping(new ShuffleGrouping(BOLT_KAFKA_READER_NAME, KafkaSpout.EVENT_SUCCESS_STREAM));


        //------------------------- Processing PresentmentMessage --------------------------------------------------------------

        //Get card authorisation data
        GBolt<?> cardAuthorisationBolt = new GRichBolt(BOLT_GET_TRANSACTIONS, new GetCardTransactions(), hintsProcessors);
        cardAuthorisationBolt.addGrouping(new ShuffleGrouping(BOLT_KAFKA_PROCESS_MESSAGE));

        //see if this is offline presentment
        GBolt<?> authValidationBolt = new GRichBolt(BOLT_CHECK_AUTHORISATION, new CheckAuthorisationBolt(), hintsProcessors);
        authValidationBolt.addGrouping(new ShuffleGrouping(BOLT_GET_TRANSACTIONS));

        //------------------------ Offline PresentmentMessage Processing -------------------------------------------------------

        //offline presentment - needs linked account in time of transaction
        GBolt<?> getLinkedAccountBolt = new GRichBolt(BOLT_GET_LINKED_ACCOUNT, new GetLinkedAccount(), hintsProcessors);
        getLinkedAccountBolt.addGrouping(new ShuffleGrouping(BOLT_CHECK_AUTHORISATION, OFFLINE_PRESENTMENT_STREAM));

        GBolt<?> validateLinikedAccountBolt = new GRichBolt(BOLT_PROCESS_LINKED_ACCOUNT, new ProcessOfflineTransactionBolt(), hintsProcessors);
        validateLinikedAccountBolt.addGrouping(new ShuffleGrouping(BOLT_GET_LINKED_ACCOUNT));

        //------------------------- Calculating Fees, Wirecard Amounts, Client Amounts -------------------------------

        GBolt<?> getFeeSchemaBolt = new GRichBolt(BOLT_GET_FEE_SCHEMA, new GetFeeSchema(), hintsProcessors);
        getFeeSchemaBolt.addGrouping(new ShuffleGrouping(BOLT_PROCESS_LINKED_ACCOUNT));
        getFeeSchemaBolt.addGrouping(new ShuffleGrouping(BOLT_CHECK_AUTHORISATION));

        //calculate client, wirecard, fees amounts
        GBolt<?> calculateAmountsBolt = new GRichBolt(BOLT_PROCESS_FEE_SCHEMA, new GenerateProcessedMessageBolt(), hintsProcessors);
        calculateAmountsBolt.addGrouping(new ShuffleGrouping(BOLT_GET_FEE_SCHEMA));

        //------------------------- Send an event with the result -------------------------------------------------------
        //GBolt<?> kafkaEventSuccessProducer = new GRichBolt("kafka-event-success-producer", new KafkaBoltFieldNameWrapper(topologyConfig.getKafkaPublisherBoltConfig(), String.class, String.class).getKafkaBolt(), 10);
        GBolt<?> kafkaEventSuccessProducer = new GRichBolt(BOLT_KAFKA_SUCCESS_PRODUCER, new KafkaBoltWrapper(topologyConfig.getKafkaPublisherBoltConfig(), String.class, String.class).getKafkaBolt(), topologyConfig.getEventResponseHints());
        kafkaEventSuccessProducer.addGrouping(new ShuffleGrouping(BOLT_PROCESS_FEE_SCHEMA));

        //-------------------------------- Error Handling --------------------------------------------------------------
        // GBolt for work with the errors
        GBolt<?> kafkaEventError = new GRichBolt(BOLT_KAFKA_EVENT_ERROR, new EventErrorBolt(), topologyConfig.getEventErrorHints());
        kafkaEventError.addGrouping(new ShuffleGrouping(BOLT_KAFKA_READER_NAME, KafkaSpout.EVENT_ERROR_STREAM));


        GBolt<?> gpsErrorBolt = new GRichBolt(BOLT_GPS_ERROR_HANDLER, new ProcessExceptionBolt(), topologyConfig.getEventErrorHints());
        gpsErrorBolt.addGrouping(new ShuffleGrouping(BOLT_KAFKA_PROCESS_MESSAGE, ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping(BOLT_GET_TRANSACTIONS, ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping(BOLT_CHECK_AUTHORISATION, ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping(BOLT_GET_LINKED_ACCOUNT, ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping(BOLT_PROCESS_LINKED_ACCOUNT, ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping(BOLT_GET_FEE_SCHEMA, ERROR_STREAM));
        gpsErrorBolt.addGrouping(new ShuffleGrouping(BOLT_PROCESS_FEE_SCHEMA, ERROR_STREAM));


        // GBolt for send errors of events to kafka
        GBolt<?> kafkaErrorProducer = new GRichBolt(BOLT_KAFKA_ERROR_PRODUCER, new KafkaBoltWrapper(topologyConfig.getKafkaPublisherErrorBoltConfig(), String.class, String.class).getKafkaBolt(), topologyConfig.getEventErrorHints());
        kafkaErrorProducer.addGrouping(new ShuffleGrouping(BOLT_KAFKA_EVENT_ERROR));
        kafkaErrorProducer.addGrouping(new ShuffleGrouping(BOLT_GPS_ERROR_HANDLER));


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
