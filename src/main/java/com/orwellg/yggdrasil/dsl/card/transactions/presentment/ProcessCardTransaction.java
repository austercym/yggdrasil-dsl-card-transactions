package com.orwellg.yggdrasil.dsl.card.transactions.presentment;


import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.dsl.card.transactions.GpsMessageException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class ProcessCardTransaction extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(ProcessCardTransaction.class);
    private PresentmentAuthorisationService presentmentService;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        presentmentService = new PresentmentAuthorisationService();
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "gpsMessage"));
        addFielsDefinition(CardPresentmentDSLTopology.OFFLINE_PRESENTMENT_STREAM, Arrays.asList("key", "processId", "eventData", "gpsMessage"));
        addFielsDefinition(CardPresentmentDSLTopology.ERROR_STREAM, Arrays.asList("key", "processId", "eventData", "exceptionMessage", "exceptionStackTrace"));
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.debug("Authorisation retrieved from database. Starting validation process");

        try{

            List<Object> inputValues = tuple.getValues();
            String key = (String) inputValues.get(0);
            String originalProcessId = (String)inputValues.get(1);
            Message eventData = (Message) inputValues.get(2);
            PresentmentMessage message = (PresentmentMessage) tuple.getValueByField("gpsMessage");
            List<CardTransaction> cardTransactions = (List<CardTransaction>) inputValues.get(3);

            CardTransaction lastTransaction = null;


            Optional<CardTransaction> max = cardTransactions.stream()
                    .filter(x -> x.getGpsTransactionId() != eventData.getTXnID())
                    .max(Comparator.comparing(CardTransaction::getTransactionTimestamp));

            if (max.isPresent()) {
                lastTransaction = max.get();

                //validate the last transaction
                List<String> validGpsMessageTypes = Arrays.asList("A", "D", "P");
                if (!validGpsMessageTypes.contains(lastTransaction.getGpsMessageType()))
                    throw new GpsMessageException("Error when processing presentment - invalid last transaction type: {} " + lastTransaction+ ". Valid types are: A, D, P");
                if (lastTransaction.getGpsTransactionId().equals(eventData.getTXnID()))
                    throw new GpsMessageException("Error when processing presentment - last processed transaction has the same transactionId: " + lastTransaction.getGpsTransactionId());
            }


            //todo: check if this is the same message - do not process messages twice

            if (lastTransaction != null){

                LOG.debug("Processing PresentmentMessage. GpsTransactionId: {}, GpsTransactionLink: {}", eventData.getTXnID(), eventData.getTransLink());

                PresentmentMessage gpsMessage = presentmentService.updatePresentment(message, lastTransaction);

                Map<String, Object> values = new HashMap<>();
                values.put("key", key);
                values.put("processId", originalProcessId);
                values.put("eventData", eventData);
                values.put("gpsMessage", gpsMessage);

                LOG.info("PresentmentMessage processed. GpsTransactionId: {}, GpsTransactionLink: {}", eventData.getTXnID(), eventData.getTransLink());
                send(tuple, values);
            }
            else {
                LOG.debug("No authorisation has been found for GpsTransactionId: {}, GpsTransactionLink: {}. Continuing with Offline PresentmentMessage flow", eventData.getTXnID(), eventData.getTransLink());

                Map<String, Object> values = new HashMap<>();
                values.put("key", key);
                values.put("processId", originalProcessId);
                values.put("eventData", eventData);
                values.put("gpsMessage", message);

                LOG.info("Offline PresentmentMessage processed. GpsTransactionId: {}, GpsTransactionLink: {}", eventData.getTXnID(), eventData.getTransLink());
                send(CardPresentmentDSLTopology.OFFLINE_PRESENTMENT_STREAM, tuple, values);
            }

        }catch (Exception e) {
            LOG.error("Error when processing PresentmentMessage Message. Tuple: {}, Message: {}, Error: {}", tuple, e.getMessage(), e);

            Map<String, Object> values = new HashMap<>();
            values.put("key", tuple.getValueByField("key"));
            values.put("processId", tuple.getValueByField("processId"));
            values.put("eventData", tuple.getValueByField("eventData"));
            values.put("exceptionMessage", ExceptionUtils.getMessage(e));
            values.put("exceptionStackTrace", ExceptionUtils.getStackTrace(e));

            send(CardPresentmentDSLTopology.ERROR_STREAM, tuple, values);
            LOG.info("Error when processing PresentmentMessage - error send to corresponded kafka topic. Tuple: {}, Message: {}, Error: {}", tuple, e.getMessage(), e);
        }
    }
}
