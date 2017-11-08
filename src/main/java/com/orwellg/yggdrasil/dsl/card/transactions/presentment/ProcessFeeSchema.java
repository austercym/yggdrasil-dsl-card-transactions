package com.orwellg.yggdrasil.dsl.card.transactions.presentment;

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
import com.orwellg.yggdrasil.dsl.card.transactions.GpsMessageException;
import com.orwellg.yggdrasil.dsl.card.transactions.services.AccountingOperationsService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.*;

public class ProcessFeeSchema extends BasicRichBolt {

    private AccountingOperationsService accountingService;
    private static final Logger LOG = LogManager.getLogger(ProcessFeeSchema.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        accountingService = new AccountingOperationsService();
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "message"));
        addFielsDefinition(CardPresentmentDSLTopology.ERROR_STREAM, Arrays.asList("key", "processId", "eventData", "exceptionMessage", "exceptionStackTrace"));
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String key = (String) tuple.getValueByField("key");
            String originalProcessId = (String) tuple.getValueByField("processId");
            Message eventData = (Message) tuple.getValueByField("eventData");
            GpsMessage presentment = (GpsMessage) tuple.getValueByField("gpsMessage");
            List<FeeSchema> schema = (List<FeeSchema>) tuple.getValueByField("retrieveValue");

            Optional<FeeSchema> maxDateOptional = schema.stream()
                    .max(Comparator.comparing(FeeSchema::getFromTimestamp));

            if (!maxDateOptional.isPresent()) {
                throw new GpsMessageException("No Fee Schema found for cardId: " + presentment.getDebitCardId() +
                        ", transactionTimestmap: " + presentment.getTransactionTimestamp());
            }

            if (schema.size() > 0) {

                //todo: validate currencies

                //prepare event to send on kafka
                GpsMessageProcessed messageProcessed = generateMessageProcessed(presentment, schema.get(0));
                Event presentmentEvent = generteEvent(this.getClass().getName()
                        , CardTransactionEvents.RESPONSE_MESSAGE.getEventName()
                        , key
                        , messageProcessed);

                Map<String, Object> values = new HashMap<>();
                values.put("key", key);
                values.put("processId", originalProcessId);
                values.put("eventData", eventData);
                values.put("message", RawMessageUtils.encodeToString(Event.SCHEMA$, presentmentEvent));

                LOG.info("PTransaction Amounts calculated. GpsTransactionId: {}, GpsTransactionLink: {}, key: {}, processId: {}", eventData.getTXnID(),
                        eventData.getTransLink(), key, originalProcessId);
                send(tuple, values);
            }

        }catch(Exception e){
            LOG.error("Error when processing Linked Accounts. Tuple: {}, Message: {}, Error: {}", tuple, e.getMessage(), e);

            Map<String, Object> values = new HashMap<>();
            values.put("key", tuple.getValueByField("key"));
            values.put("processId", tuple.getValueByField("processId"));
            values.put("eventData", tuple.getValueByField("eventData"));
            values.put("exceptionMessage", e.getMessage());
            values.put("exceptionStackTrace", e.getStackTrace());

            send(CardPresentmentDSLTopology.ERROR_STREAM, tuple, values);
            LOG.info("Error when processing Linked Accounts - error send to corresponded kafka topic. Tuple: {}, Message: {}, Error: {}", tuple, e.getMessage(), e);
        }
    }


    private GpsMessageProcessed generateMessageProcessed(GpsMessage presentment, FeeSchema feeSchema){

        LOG.debug("Generating gpsMessageProcessed message");

        BigDecimal feesAmount = accountingService.getFeeAmount(feeSchema, presentment.getSettlementAmount());
        BigDecimal blockedClientAmount = presentment.getSettlementAmount().add(feesAmount).negate();

        GpsMessageProcessed gpsMessageProcessed = new GpsMessageProcessed();
        gpsMessageProcessed.setGpsTransactionLink(presentment.getGpsTransactionLink());
        gpsMessageProcessed.setGpsTransactionId(presentment.getGpsTransactionId());
        gpsMessageProcessed.setDebitCardId(presentment.getDebitCardId());
        gpsMessageProcessed.setTransactionTimestamp(presentment.getTransactionTimestamp().getTime() / 1000L); //todo: helper to
        gpsMessageProcessed.setGpsTransactionTime(presentment.getGpsTrnasactionDate().getTime() / 1000L);
        gpsMessageProcessed.setInternalAccountId(presentment.getInternalAccountId());
        gpsMessageProcessed.setWirecardAmount(presentment.getSettlementAmount().doubleValue());
        gpsMessageProcessed.setWirecardCurrency(presentment.getSettlementCurrency());
        gpsMessageProcessed.setBlockedClientAmount(blockedClientAmount.doubleValue());
        gpsMessageProcessed.setBlockedClientCurrency(presentment.getSettlementCurrency());
        gpsMessageProcessed.setFeesAmount(feesAmount.doubleValue());
        gpsMessageProcessed.setFeesCurrency(presentment.getInternalAccountCurrency());
        gpsMessageProcessed.setGpsMessageType(presentment.getGpsMessageType());
        gpsMessageProcessed.setInternalAccountCurrency(presentment.getInternalAccountCurrency());

        double authBlockedAmount = presentment.getAuthBlockedClientAmount() == null ? 0.0 : presentment.getAuthBlockedClientAmount().doubleValue();
        gpsMessageProcessed.setAppliedBlockedClientAmount(authBlockedAmount);
        gpsMessageProcessed.setAppliedBlockedClientCurrency(presentment.getAuthBlockedClientCurrency());
        double authWirecardAmount = presentment.getAuthWirecardAmount() == null ? 0.0 : presentment.getAuthWirecardAmount().doubleValue();
        gpsMessageProcessed.setAppliedWirecardAmount(authWirecardAmount);
        gpsMessageProcessed.setAppliedWirecardCurrency(presentment.getAuthWirecardCurrency());
        double authFeeAmount = presentment.getAuthFeeAmount() == null ? 0.0 : presentment.getAuthFeeAmount().doubleValue();
        gpsMessageProcessed.setAppliedFeesAmount(authFeeAmount);
        gpsMessageProcessed.setAppliedFeesCurrency(presentment.getAuthFeeCurrency());

        LOG.debug("Message generated. Parameters: {}", gpsMessageProcessed);

        return gpsMessageProcessed;
    }

    private Event generteEvent(String eventName, String source, String parentKey, Object eventData){

        LOG.debug("Generating event with presentment data");

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

        LOG.debug("Eevent with presentment data generated correctly. Parameters: {}", eventData);

        return event;
    }

}
