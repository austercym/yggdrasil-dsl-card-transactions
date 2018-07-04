package com.orwellg.yggdrasil.dsl.card.transactions.presentment.bolts;


import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.card.transaction.commons.model.TransactionInfo;
import com.orwellg.yggdrasil.dsl.card.transactions.presentment.PresentmentTopology;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CheckAuthorisationBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(CheckAuthorisationBolt.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA, Fields.TRANSACTION_LIST));
        addFielsDefinition(PresentmentTopology.OFFLINE_PRESENTMENT_STREAM, Arrays.asList(Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA));
    }

    @Override
    public void execute(Tuple tuple) {

        try{

            String key = (String) tuple.getValueByField(Fields.KEY);
            String originalProcessId = (String)tuple.getValueByField(Fields.PROCESS_ID);

            LOG.debug("Key: {} | ProcessId: {} | Card Transactions retrieved from database. Starting validation process", key, originalProcessId);

            TransactionInfo eventData = (TransactionInfo) tuple.getValueByField(Fields.EVENT_DATA);
            List<CardTransaction> cardTransactions = (List<CardTransaction>) tuple.getValueByField("retrieveValue");

            if (cardTransactions != null && !cardTransactions.isEmpty()) {
                LOG.info("Key: {} | ProcessId: {} | Processing Online Presentment. ProviderMessageId: {}, ProviderTransactionId: {}", key, originalProcessId, eventData.getProviderMessageId(), eventData.getProviderTransactionId());

                Map<String, Object> values = getReturnValues(key, originalProcessId, eventData);
                values.put(Fields.TRANSACTION_LIST, cardTransactions);
                send(tuple, values);
            }
            else {
                LOG.info("Key: {} | ProcessId: {} | Processing Offline Presentment. ProviderMessageId: {}, ProviderTransactionId: {}. Continuing with Offline PresentmentMessage flow", key, originalProcessId, eventData.getProviderMessageId(), eventData.getProviderTransactionId());
                Map<String, Object> values = getReturnValues(key, originalProcessId, eventData);
                send(PresentmentTopology.OFFLINE_PRESENTMENT_STREAM, tuple, values);
            }

        }catch (Exception e) {
            LOG.error("Error when processing PresentmentMessage Message. Tuple: {}, Message: {}, Error: {}", tuple, e.getMessage(), e);
            error(e, tuple);
        }
    }

    private Map<String, Object> getReturnValues(String key, String originalProcessId, TransactionInfo eventData) {

        Map<String, Object> values = new HashMap<>();
        values.put(Fields.KEY, key);
        values.put(Fields.PROCESS_ID, originalProcessId);
        values.put(Fields.EVENT_DATA, eventData);
        return values;
    }
}
