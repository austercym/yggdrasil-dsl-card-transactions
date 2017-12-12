package com.orwellg.yggdrasil.dsl.card.transactions.presentment;


import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.dsl.card.transactions.model.PresentmentMessage;
import com.orwellg.yggdrasil.dsl.card.transactions.presentment.services.AuthorisationValidationService;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class CheckAuthorisationBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(CheckAuthorisationBolt.class);
    private AuthorisationValidationService authorisationService;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        authorisationService = new AuthorisationValidationService();
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "gpsMessage"));
        addFielsDefinition(CardPresentmentDSLTopology.OFFLINE_PRESENTMENT_STREAM, Arrays.asList("key", "processId", "eventData", "gpsMessage"));
    }

    @Override
    public void execute(Tuple tuple) {

        try{

            String key = (String) tuple.getValueByField("key");
            String originalProcessId = (String)tuple.getValueByField("processId");

            LOG.debug("Key: {} | ProcessId: {} | Card Transactions retrieved from database. Starting validation process", key, originalProcessId);

            Message eventData = (Message) tuple.getValueByField("eventData");
            PresentmentMessage message = (PresentmentMessage) tuple.getValueByField("gpsMessage");
            List<CardTransaction> cardTransactions = (List<CardTransaction>) tuple.getValueByField("retrieveValue");

            CardTransaction lastTransaction = authorisationService.getLast(cardTransactions, eventData.getTXnID());

            if (lastTransaction != null){
                LOG.info("Key: {} | ProcessId: {} | Processing Online Presentment. GpsTransactionId: {}, GpsTransactionLink: {}", key, originalProcessId, eventData.getTXnID(), eventData.getTransLink());

                message = message.UpdateWithAuthorisationData(lastTransaction);
                Map<String, Object> values = getReturnValues(key, originalProcessId, eventData, message);
                send(tuple, values);
            }
            else {
                LOG.info("Key: {} | ProcessId: {} | Processing Offline Presentment. GpsTransactionId: {}, GpsTransactionLink: {}. Continuing with Offline PresentmentMessage flow", key, originalProcessId, eventData.getTXnID(), eventData.getTransLink());
                Map<String, Object> values = getReturnValues(key, originalProcessId, eventData, message);
                send(CardPresentmentDSLTopology.OFFLINE_PRESENTMENT_STREAM, tuple, values);
            }

        }catch (Exception e) {
            LOG.error("Error when processing PresentmentMessage Message. Tuple: {}, Message: {}, Error: {}", tuple, e.getMessage(), e);
            error(e, tuple);
        }
    }

    private Map<String, Object> getReturnValues(String key, String originalProcessId, Message eventData, PresentmentMessage presentmentMessage){

        Map<String, Object> values = new HashMap<>();
        values.put("key", key);
        values.put("processId", originalProcessId);
        values.put("eventData", eventData);
        values.put("gpsMessage", presentmentMessage);
        return values;
    }
}
