package com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.processors.presentment;

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
import com.orwellg.yggdrasil.dsl.card.transactions.GpsMessage;
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

public class PresentmentCalculateAmountsBolt extends BasicRichBolt {

    private AccountingOperationsService accountingService;
    private static final Logger LOG = LogManager.getLogger(PresentmentCalculateAmountsBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        accountingService = new AccountingOperationsService();
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "message"));
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
                throw new GpsMessageException(String.format("No Fee Schema found for cardId: {}, transactionTimestmap: {}",
                        presentment.getDebitCardId(), presentment.getTransactionTimestamp()));
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
            LOG.error("Error when calculating transaction amounts. Tuple: {}, Message: {}, Error: {}", tuple, e.getMessage(), e);
            error(e, tuple);
        }
    }


    private GpsMessageProcessed generateMessageProcessed(GpsMessage presentment, FeeSchema feeSchema){

        LOG.debug("Generating gpsMessageProcessed message");
        BigDecimal wirecardAmount =
                accountingService.calculateWirecardAmount(presentment.getAuthWirecardAmount(), presentment.getSettlementAmount());
        BigDecimal clientAmount =
                accountingService.calculateBlockedClientAmount(presentment.getAuthBlockedClientAmount(), presentment.getSettlementAmount(), feeSchema);
        BigDecimal feesAmount = accountingService.getFeeAmount(feeSchema, presentment.getSettlementAmount());
        BigDecimal calculatedFees = accountingService.calculateFees(presentment.getAuthFeeAmount(), feesAmount);

        GpsMessageProcessed gpsMessageProcessed = new GpsMessageProcessed();
        gpsMessageProcessed.setGpsTransactionLink(presentment.getGpsTransactionLink());
        gpsMessageProcessed.setGpsTransactionId(presentment.getGpsTransactionId());
        gpsMessageProcessed.setDebitCardId(presentment.getDebitCardId());
        gpsMessageProcessed.setTransactionTimestamp(presentment.getTransactionTimestamp().toString());      //todo: is this a correct field?
        gpsMessageProcessed.setInternalAccountId(presentment.getInternalAccountId());
        gpsMessageProcessed.setWirecardAmount(wirecardAmount.doubleValue());
        gpsMessageProcessed.setWirecardCurrency(presentment.getAuthWirecardCurrency());
        gpsMessageProcessed.setBlockedClientAmount(clientAmount.doubleValue());
        gpsMessageProcessed.setBlockedClientCurrency(presentment.getSettlementCurrency());
        gpsMessageProcessed.setFeesAmount(calculatedFees.doubleValue());
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
