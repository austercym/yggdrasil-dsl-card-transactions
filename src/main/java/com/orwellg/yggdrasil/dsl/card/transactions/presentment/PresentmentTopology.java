package com.orwellg.yggdrasil.dsl.card.transactions.presentment;

import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.storm.topology.TopologyFactory;
import com.orwellg.umbrella.commons.storm.topology.component.base.AbstractTopology;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.EventErrorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaEventGeneratorBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.bolt.GRichBolt;
import com.orwellg.umbrella.commons.storm.topology.generic.grouping.ShuffleGrouping;
import com.orwellg.umbrella.commons.storm.topology.generic.spout.GSpout;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaBoltWrapper;
import com.orwellg.umbrella.commons.storm.wrapper.kafka.KafkaSpoutWrapper;
import com.orwellg.yggdrasil.dsl.card.transactions.common.bolts.EventToTransactionInfoBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.presentment.bolts.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.generated.StormTopology;

import java.util.Arrays;

public class PresentmentTopology extends AbstractTopology {

    private final static Logger LOG = LogManager.getLogger(PresentmentTopology.class);
  
    public final static String OFFLINE_PRESENTMENT_STREAM = "offline-presentment-stream";
    public static final String TOPOLOGY_NAME = "yggdrasil-card-presentment";

    private final static String BOLT_KAFKA_READER_NAME = "kafka-event-reader";
    private final static String BOLT_KAFKA_PROCESS_MESSAGE = "process-kafka-message";
    private final static String BOLT_GET_TRANSACTIONS = "get-card-transactions";
    private final static String BOLT_CHECK_AUTHORISATION = "check-authorisation";
    private final static String BOLT_GET_LINKED_ACCOUNT = "get-linked-account";
    private final static String BOLT_PROCESS_LINKED_ACCOUNT = "process-linked-account";
    private final static String BOLT_PROCESS_FEE_SCHEMA = "process-fee-schema";
    private final static String BOLT_KAFKA_SUCCESS_PRODUCER = "kafka-success-producer";
    private final static String BOLT_KAFKA_EVENT_ERROR = "kafka-event-error-process";
    private final static String BOLT_KAFKA_ERROR_PRODUCER = "kafka-error-producer";
    private static final String EVENT_GENERATOR = "EventGenerator";
    static final String PROPERTIES_FILE = "presentment-topology.properties";

    @Override
    public StormTopology load() {
        LOG.debug("Creating Card Presentments processing topology");

        TopologyConfig config = TopologyConfigFactory.getTopologyConfig(PROPERTIES_FILE);
        int hintsSpout = config.getKafkaSpoutHints();
        int hintsProcessors = config.getActionBoltHints();

        // Create the spout that read the events from Kafka
        GSpout kafkaEventReader = new GSpout(BOLT_KAFKA_READER_NAME,
                new KafkaSpoutWrapper( config.getKafkaSubscriberSpoutConfig(), String.class, String.class).getKafkaSpout(), hintsSpout);

        // Parse the events and we send it to the rest of the topology
        GBolt<?> kafkaEventProcess = new GRichBolt(BOLT_KAFKA_PROCESS_MESSAGE, new EventToTransactionInfoBolt(), hintsProcessors);
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

        GBolt<?> validateLinkedAccountBolt = new GRichBolt(BOLT_PROCESS_LINKED_ACCOUNT, new ProcessOfflineTransactionBolt(), hintsProcessors);
        validateLinkedAccountBolt.addGrouping(new ShuffleGrouping(BOLT_GET_LINKED_ACCOUNT));

        //------------------------- Calculating Wirecard Amounts, Client Amounts -------------------------------

        GBolt<?> calculateAmountsBolt = new GRichBolt(BOLT_PROCESS_FEE_SCHEMA, new GenerateProcessedMessageBolt(), hintsProcessors);
        calculateAmountsBolt.addGrouping(new ShuffleGrouping(BOLT_PROCESS_LINKED_ACCOUNT));
        calculateAmountsBolt.addGrouping(new ShuffleGrouping(BOLT_CHECK_AUTHORISATION));

        GBolt<?> eventGeneratorBolt = new GRichBolt(EVENT_GENERATOR, new KafkaEventGeneratorBolt(), config.getActionBoltHints());
        eventGeneratorBolt.addGrouping(new ShuffleGrouping(BOLT_PROCESS_FEE_SCHEMA));

        //------------------------- Send an event with the result -------------------------------------------------------

        GBolt<?> kafkaEventSuccessProducer = new GRichBolt(BOLT_KAFKA_SUCCESS_PRODUCER, new KafkaBoltWrapper(config.getKafkaPublisherBoltConfig(), String.class, String.class).getKafkaBolt(), config.getEventResponseHints());
        kafkaEventSuccessProducer.addGrouping(new ShuffleGrouping(EVENT_GENERATOR));

        //-------------------------------- Error Handling --------------------------------------------------------------
        // GBolt for work with the errors
        GBolt<?> kafkaEventError = new GRichBolt(BOLT_KAFKA_EVENT_ERROR, new EventErrorBolt(), config.getEventErrorHints());
        kafkaEventError.addGrouping(new ShuffleGrouping(BOLT_KAFKA_READER_NAME, KafkaSpout.EVENT_ERROR_STREAM));

        // GBolt for send errors of events to kafka
        GBolt<?> kafkaErrorProducer = new GRichBolt(BOLT_KAFKA_ERROR_PRODUCER, new KafkaBoltWrapper(config.getKafkaPublisherErrorBoltConfig(), String.class, String.class).getKafkaBolt(), config.getEventErrorHints());
        kafkaErrorProducer.addGrouping(new ShuffleGrouping(BOLT_KAFKA_EVENT_ERROR));


        // Build the topology
        StormTopology topology = TopologyFactory.generateTopology(
                kafkaEventReader,
                Arrays.asList(kafkaEventProcess, cardAuthorisationBolt, authValidationBolt,
                        getLinkedAccountBolt, validateLinkedAccountBolt, calculateAmountsBolt, eventGeneratorBolt, kafkaEventSuccessProducer, kafkaEventError, kafkaErrorProducer));
        return topology;
    }

    @Override
    public String name() {
        return TOPOLOGY_NAME;
    }
}
