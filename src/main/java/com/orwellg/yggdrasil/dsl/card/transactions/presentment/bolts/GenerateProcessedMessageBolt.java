package com.orwellg.yggdrasil.dsl.card.transactions.presentment.bolts;

import com.orwellg.umbrella.avro.types.event.EntityIdentifierType;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.event.EventType;
import com.orwellg.umbrella.avro.types.event.ProcessIdentifierType;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.FeeSchema;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.yggdrasil.dsl.card.transactions.model.GpsMessageProcessingException;
import com.orwellg.yggdrasil.dsl.card.transactions.model.PresentmentErrorCode;
import com.orwellg.yggdrasil.dsl.card.transactions.model.PresentmentMessage;
import com.orwellg.yggdrasil.dsl.card.transactions.presentment.services.FeeValidationService;
import com.orwellg.yggdrasil.dsl.card.transactions.presentment.services.ResponseService;
import com.orwellg.yggdrasil.dsl.card.transactions.services.AccountingOperationsService;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import com.orwellg.yggdrasil.net.client.producer.CommandProducerConfig;
import com.orwellg.yggdrasil.net.client.producer.GeneratorIdCommandProducer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;

public class GenerateProcessedMessageBolt extends BasicRichBolt {

    private FeeValidationService feeValidatorService;
    private AccountingOperationsService accountingService;
    private ResponseService responseService;
    private GeneratorIdCommandProducer idGeneratorClient;
    private static final Logger LOG = LogManager.getLogger(GenerateProcessedMessageBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        accountingService = new AccountingOperationsService();
        feeValidatorService = new FeeValidationService();
        responseService = new ResponseService();
        initializeIdGeneratorClient();
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "message"));
    }

    @Override
    public void execute(Tuple tuple) {

        String key = (String) tuple.getValueByField("key");
        String originalProcessId = (String) tuple.getValueByField("processId");
        Message eventData = (Message) tuple.getValueByField("eventData");

        LOG.debug("Key: {} | ProcessId: {} | Preparing Response. GpsTransactionId: {}, GpsTransactionLink: {}", key, originalProcessId, eventData.getTXnID(),
                eventData.getTransLink());

        try {

            PresentmentMessage presentment = (PresentmentMessage) tuple.getValueByField("gpsMessage");
            List<FeeSchema> schema = (List<FeeSchema>) tuple.getValueByField("retrieveValue");

            FeeSchema fees = feeValidatorService.getLast(schema);

            if(fees == null){
                throw new GpsMessageProcessingException(PresentmentErrorCode.FEE_SCHEMA_MISSING,
                        "No Fee Schema found for cardId: " + presentment.getDebitCardId() +
                                    ", transactionTimestmap: " + presentment.getTransactionTimestamp());
            }
            LOG.debug("Key: {} | ProcessId: {} | Selected Fee Schema. Percentge: {}, Amount: {}, Type: {}, ",
                    key, originalProcessId, fees.getPercentage(), fees.getAmount(), fees.getFeeType());

            BigDecimal settlementAmount = presentment.getSettlementAmount();
            presentment.setFeeAmount(accountingService.getFeeAmount(fees, settlementAmount));
            presentment.setBlockedClientAmount(accountingService.calculateBlockedClientAmount(settlementAmount, presentment.getFeeAmount()));

            GpsMessageProcessed messageProcessed = responseService.generateResponse(presentment);
            Event presentmentEvent = generateEvent(this.getClass().getName()
                        , CardTransactionEvents.RESPONSE_MESSAGE.getEventName()
                        , key
                        , messageProcessed);

            Map<String, Object> values = new HashMap<>();
            values.put("key", key);
            values.put("processId", originalProcessId);
            values.put("eventData", eventData);
            values.put("message", RawMessageUtils.encodeToString(Event.SCHEMA$, presentmentEvent));

            send(tuple, values);
            LOG.info(" Key: {} | ProcessId: {} | GPS Presentment Message Processed. Response sent to kafka topic. GpsTransactionId: {}, Gps TransactionLink: {}",
                    key, originalProcessId, eventData.getTXnID(),  eventData.getTransLink());

        }catch(Exception e){
            LOG.error("Error when generating response message. Tuple: {}, Message: {}, Error: {}", tuple, e.getMessage(), e);
            error(e, tuple);
        }
    }

    private void initializeIdGeneratorClient() {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name());
        props.setProperty(CommandProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ComponentFactory.getConfigurationParams().getZookeeperConnection());
        props.setProperty(CommandProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(CommandProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        idGeneratorClient = new GeneratorIdCommandProducer(new CommandProducerConfig(props), 1, Time.SYSTEM);
    }

    private Event generateEvent(String eventName, String source, String parentKey, Object eventData){

        LOG.trace("Generating event with presentment data");

        String uuid = retrieveResponseKey();

        // Create the event type
        EventType eventType = new EventType();
        eventType.setName(eventName);
        eventType.setVersion(Constants.getDefaultEventVersion()); //??
        eventType.setKey(uuid);
        eventType.setSource(source);
        eventType.setParentKey(parentKey);
        SimpleDateFormat format = new SimpleDateFormat(Constants.getDefaultEventTimestampFormat());
        eventType.setTimestamp(format.format(new Date()));
        eventType.setData(eventData.toString());

        ProcessIdentifierType processIdentifier = new ProcessIdentifierType();
        processIdentifier.setUuid(uuid);

        EntityIdentifierType entityIdentifier = new EntityIdentifierType();
        entityIdentifier.setEntity(Constants.IPAGOO_ENTITY);
        entityIdentifier.setBrand(Constants.IPAGOO_BRAND);

        // Create the correspondent event
        Event event = new Event();
        event.setEvent(eventType);
        event.setProcessIdentifier(processIdentifier);
        event.setEntityIdentifier(entityIdentifier);

        LOG.trace("Event with presentment data generated correctly. Parameters: {}", eventData);

        return event;
    }

    private String retrieveResponseKey() {

        LOG.debug("Retrieving response message key ...");
        String id;
        try {
            id = idGeneratorClient.getGeneralUniqueId();
        } catch (Exception e) {
            LOG.error("Id generator client exception (a random id has been generated locally) - {}", e.getMessage(), e);
            id = UUID.randomUUID().toString() + "_" + Instant.now().hashCode();
        }
        return id;
    }
}
