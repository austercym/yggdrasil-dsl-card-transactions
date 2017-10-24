package com.yggdrasil.dsl.card.transactions.topology.bolts.event;

import com.orwellg.umbrella.avro.types.event.EntityIdentifierType;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.event.EventType;
import com.orwellg.umbrella.avro.types.event.ProcessIdentifierType;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.yggdrasil.dsl.card.transactions.topology.CardPresentmentDSLTopology;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;
import java.text.SimpleDateFormat;
import java.util.*;

public class PresentmentValidateAuthorisationBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(PresentmentValidateAuthorisationBolt.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "message"));
        addFielsDefinition(CardPresentmentDSLTopology.OFFLINE_PRESENTMENT_STREAM, Arrays.asList("key", "processId", "eventData", "message"));
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.debug("Authorisation retrieved from database. Starting validation proccess");

        try{

            List<Object> inputValues = tuple.getValues();
            String key = (String) inputValues.get(0);
            String originalProcessId = (String)inputValues.get(1);
            Message eventData = (Message) inputValues.get(2);
            List<CardTransaction> cardTransactions = (List<CardTransaction>) inputValues.get(3);

            CardTransaction lastTransaction = null;
            Optional<CardTransaction> max = cardTransactions.stream()
                    .filter(x -> x.getGpsTransactionId() == eventData.getTXnID())
                    .max(Comparator.comparing(CardTransaction::getTransactionTimestamp));
            if (max.isPresent()){
                lastTransaction = max.get();
            }

            //todo: check if this is the same message - do not process messages twice

            if (lastTransaction != null){

                LOG.debug("Processing Presentment. GpsTransactionId: {}, GpsTransactionLink: {}", eventData.getTXnID(), eventData.getTransLink());

                GpsMessageProcessed message = generateMessageProcessed(eventData, lastTransaction);
                Event presentmentEvent = generteEvent(this.getClass().getName()
                                                     ,CardTransactionEvents.RESPONSE_MESSAGE.getEventName()
                                                     ,key
                                                     ,message);

                Map<String, Object> values = new HashMap<>();
                values.put("key", key);
                values.put("processId", originalProcessId);
                values.put("eventData", eventData);
                values.put("message", RawMessageUtils.encodeToString(Event.SCHEMA$, presentmentEvent));

                LOG.info("Presentment processed. GpsTransactionId: {}, GpsTransactionLink: {}", eventData.getTXnID(), eventData.getTransLink());
                send(tuple, values);
            }
            else {
                LOG.debug("No authorisation has been found for GpsTransactionId: {}, GpsTransactionLink: {}. Continuing with Offline Presentment flow", eventData.getTXnID(), eventData.getTransLink());

                Map<String, Object> values = new HashMap<>();
                values.put("key", key);
                values.put("processId", originalProcessId);
                values.put("eventData", eventData);
                values.put("message", null);

                LOG.info("Offline Presentment processed. GpsTransactionId: {}, GpsTransactionLink: {}", eventData.getTXnID(), eventData.getTransLink());
                send(CardPresentmentDSLTopology.OFFLINE_PRESENTMENT_STREAM, tuple, values);
            }

        }catch (Exception e) {
            //todo: error
            LOG.error("The received event {} can not be decoded. Message: {}", tuple, e.getMessage(), e);
            error(e, tuple);
        }


    }

    private GpsMessageProcessed generateMessageProcessed(Message eventData, CardTransaction lastTransaction){

        LOG.debug("Generating gpsMessageProcessed message");

        //todo: check currencies -> if different - error
        double appliedWirecardAmount = lastTransaction.getWirecardAmount().doubleValue();
        if (appliedWirecardAmount == Double.NEGATIVE_INFINITY || appliedWirecardAmount == Double.POSITIVE_INFINITY){
            throw new UnsupportedOperationException("Error when generating gpsMessageProcessed. AppliedWirecardAmount can't be converted to double. Value: " + lastTransaction.getWirecardAmount());
        }

        double appliedBlockedBalance = lastTransaction.getBlockedClientAmount().doubleValue();
        if (appliedBlockedBalance == Double.NEGATIVE_INFINITY || appliedBlockedBalance == Double.POSITIVE_INFINITY){
            throw new UnsupportedOperationException("Error when generating gpsMessageProcessed. AppliedBlockedBalance can't be converted to double. Value: " + lastTransaction.getBlockedClientAmount());
        }


        double wirecardDiff = appliedWirecardAmount - eventData.getSettleAmt();
        double clientAmtDiff = appliedBlockedBalance - eventData.getTxnAmt();

        GpsMessageProcessed gpsMessageProcessed = new GpsMessageProcessed();
        gpsMessageProcessed.setGpsTransactionLink(eventData.getTransLink());
        gpsMessageProcessed.setGpsTransactionId(eventData.getTXnID());
        gpsMessageProcessed.setDebitCardId(lastTransaction.getDebitCardId());
        gpsMessageProcessed.setTransactionTimestamp(eventData.getTxnGPSDate().toString());      //todo: is this a correct field?
        gpsMessageProcessed.setInternalAccountId(lastTransaction.getInternalAccountId());
        gpsMessageProcessed.setOriginalWirecardAmount(eventData.getSettleAmt());
        gpsMessageProcessed.setWirecardAmount(wirecardDiff);
        gpsMessageProcessed.setWirecardCurrency(eventData.getSettleCcy());
        gpsMessageProcessed.setAppliedWirecardAmount(lastTransaction.getWirecardAmount().doubleValue());
        gpsMessageProcessed.setBlockedClientAmount(clientAmtDiff);
        gpsMessageProcessed.setBlockedClientCurrency(eventData.getTxnCCy());
        gpsMessageProcessed.setAppliedBlockedClientAmount(lastTransaction.getBlockedClientAmount().doubleValue());

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
