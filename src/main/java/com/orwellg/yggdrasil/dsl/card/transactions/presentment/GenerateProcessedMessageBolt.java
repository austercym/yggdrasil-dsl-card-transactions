package com.orwellg.yggdrasil.dsl.card.transactions.presentment;

import com.orwellg.umbrella.avro.types.cards.SpendGroup;
import com.orwellg.umbrella.avro.types.event.EntityIdentifierType;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.event.EventType;
import com.orwellg.umbrella.avro.types.event.ProcessIdentifierType;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.FeeSchema;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionType;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.yggdrasil.dsl.card.transactions.model.GpsMessageProcessingException;
import com.orwellg.yggdrasil.dsl.card.transactions.model.PresentmentErrorCode;
import com.orwellg.yggdrasil.dsl.card.transactions.model.PresentmentMessage;
import com.orwellg.yggdrasil.dsl.card.transactions.presentment.services.FeeValidationService;
import com.orwellg.yggdrasil.dsl.card.transactions.presentment.services.ResponseService;
import com.orwellg.yggdrasil.dsl.card.transactions.services.AccountingOperationsService;
import com.orwellg.yggdrasil.dsl.card.transactions.services.FeeValidatorService;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

public class GenerateProcessedMessageBolt extends BasicRichBolt {

    private FeeValidationService feeValidatorService;
    private AccountingOperationsService accountingService;
    private ResponseService responseService;
    private static final Logger LOG = LogManager.getLogger(GenerateProcessedMessageBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        accountingService = new AccountingOperationsService();
        feeValidatorService = new FeeValidationService();
        responseService = new ResponseService();
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
            Event presentmentEvent = generteEvent(this.getClass().getName()
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

    private Event generteEvent(String eventName, String source, String parentKey, Object eventData){

        LOG.trace("Generating event with presentment data");

        //todo: should we use the key generator service?
        String uuid = UUID.randomUUID().toString();

        // Create the event type
        EventType eventType = new EventType();
        eventType.setName(eventName);
        eventType.setVersion(Constants.getDefaultEventVersion()); //??
        eventType.setParentKey(Constants.EMPTY);
        eventType.setKey("EVENT-" + uuid); //todo: should this be the key?
        eventType.setSource(source);
        eventType.setParentKey(parentKey);
        SimpleDateFormat format = new SimpleDateFormat(Constants.getDefaultEventTimestampFormat());
        eventType.setTimestamp(format.format(new Date()));
        eventType.setData(eventData.toString());

        ProcessIdentifierType processIdentifier = new ProcessIdentifierType();
        processIdentifier.setUuid("PROCESS-" + uuid);

        EntityIdentifierType entityIdentifier = new EntityIdentifierType();
        entityIdentifier.setEntity(Constants.IPAGOO_ENTITY);
        entityIdentifier.setBrand(Constants.IPAGOO_BRAND);

        // Create the correspondent event
        Event event = new Event();
        event.setEvent(eventType);
        event.setProcessIdentifier(processIdentifier);
        event.setEntityIdentifier(entityIdentifier);

        LOG.trace("Eevent with presentment data generated correctly. Parameters: {}", eventData);

        return event;
    }

}
