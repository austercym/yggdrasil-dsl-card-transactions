package com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.processors.presentment;


import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.dsl.card.transactions.GpsMessage;
import com.orwellg.yggdrasil.dsl.card.transactions.services.ParseMessageService;
import com.orwellg.yggdrasil.dsl.card.transactions.topology.CardPresentmentDSLTopology;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;
import java.util.*;

public class PresentmentValidateAuthorisationBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(PresentmentValidateAuthorisationBolt.class);
    private static final ParseMessageService parseMessageService = new ParseMessageService();

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "gpsMessage"));
        addFielsDefinition(CardPresentmentDSLTopology.OFFLINE_PRESENTMENT_STREAM, Arrays.asList("key", "processId", "eventData", "gpsMessage"));
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.debug("Authorisation retrieved from database. Starting validation process");

        try{

            List<Object> inputValues = tuple.getValues();
            String key = (String) inputValues.get(0);
            String originalProcessId = (String)inputValues.get(1);
            Message eventData = (Message) inputValues.get(2);
            List<CardTransaction> cardTransactions = (List<CardTransaction>) inputValues.get(3);

            CardTransaction lastTransaction = null;
            Optional<CardTransaction> max = cardTransactions.stream()
                    .filter(x -> x.getGpsTransactionId() != eventData.getTXnID())
                    .max(Comparator.comparing(CardTransaction::getTransactionTimestamp));
            if (max.isPresent()){
                lastTransaction = max.get();
            }


            //todo: check if this is the same message - do not process messages twice

            if (lastTransaction != null){

                LOG.debug("Processing GpsMessage. GpsTransactionId: {}, GpsTransactionLink: {}", eventData.getTXnID(), eventData.getTransLink());

                GpsMessage gpsMessage = mapToPresentment(eventData, lastTransaction);

                Map<String, Object> values = new HashMap<>();
                values.put("key", key);
                values.put("processId", originalProcessId);
                values.put("eventData", eventData);
                values.put("gpsMessage", gpsMessage);

                LOG.info("GpsMessage processed. GpsTransactionId: {}, GpsTransactionLink: {}", eventData.getTXnID(), eventData.getTransLink());
                send(tuple, values);
            }
            else {
                LOG.debug("No authorisation has been found for GpsTransactionId: {}, GpsTransactionLink: {}. Continuing with Offline GpsMessage flow", eventData.getTXnID(), eventData.getTransLink());

                GpsMessage gpsMessage = mapToPresentment(eventData, lastTransaction);

                Map<String, Object> values = new HashMap<>();
                values.put("key", key);
                values.put("processId", originalProcessId);
                values.put("eventData", eventData);
                values.put("gpsMessage", gpsMessage);

                LOG.info("Offline GpsMessage processed. GpsTransactionId: {}, GpsTransactionLink: {}", eventData.getTXnID(), eventData.getTransLink());
                send(CardPresentmentDSLTopology.OFFLINE_PRESENTMENT_STREAM, tuple, values);
            }

        }catch (Exception e) {
            //todo: error
            LOG.error("Error when processing GpsMessage Message. Tuple: {}, Message: {}, Error: {}", tuple, e.getMessage(), e);
            error(e, tuple);
        }


    }

    private GpsMessage mapToPresentment(Message message, CardTransaction transaction){

        GpsMessage presentment = parseMessageService.parse(message);
        if (transaction != null){
            presentment.setAuthBlockedClientAmaount(transaction.getBlockedClientAmount());
            presentment.setAuthBlockedClientCurrency(transaction.getBlockedClientCurrency());
            presentment.setAuthWirecardAmount(transaction.getWirecardAmount());
            presentment.setAuthWirecardCurrency(transaction.getWirecardCurrency());
            presentment.setInternalAccountId(transaction.getInternalAccountId());
            presentment.setInternalAccountCurrency(transaction.getInternalAccountCurrency()); //todo:??
            presentment.setAuthFeeAmount(transaction.getFeeAmount());
            presentment.setAuthFeeCurrency(transaction.getInternalAccountCurrency()); //todo: what currency should we get fees?
        }
        return presentment;
    }



}
