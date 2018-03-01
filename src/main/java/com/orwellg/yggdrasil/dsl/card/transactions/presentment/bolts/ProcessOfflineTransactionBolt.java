package com.orwellg.yggdrasil.dsl.card.transactions.presentment.bolts;

import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.LinkedAccount;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessOfflineTransactionBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(ProcessOfflineTransactionBolt.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA, Fields.LINKED_ACCOUNT));
    }

    @Override
    public void execute(Tuple tuple) {

        try {
            String key = tuple.getStringByField(Fields.KEY);
            String processId = tuple.getStringByField(Fields.PROCESS_ID);

            LOG.debug("Key: {} | ProcessId: {} | Processing offline transaction", key, processId);

            TransactionInfo presentment = (TransactionInfo) tuple.getValueByField(Fields.EVENT_DATA);
            List<LinkedAccount> linkedAccounts = (List<LinkedAccount>) tuple.getValueByField("retrieveValue");

            LOG.debug("Key: {} | ProcessId: {} | " + "Selecting linked account. CardTransactionId: {}, Transaction date: {}", key, processId,
                    presentment.getDebitCardId(), presentment.getTransactionDateTime());

            if (linkedAccounts == null || linkedAccounts.isEmpty()) {
                throw new Exception("No linked accounts found for cardId: " + presentment.getDebitCardId() + " transaction date time: " + presentment.getTransactionDateTime());
            }
            LinkedAccount linkedAccount = linkedAccounts.get(0);


            LOG.debug("Key: {} | ProcessId: {} | Selected Linked Account. LinkedAccountId: {}, FromTimestamp: {}", key, processId,
                   linkedAccount.getInternalAccountId(), linkedAccount.getFromTimestamp());

            //3. send message to further processing
            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_DATA, presentment);
            values.put(Fields.LINKED_ACCOUNT, linkedAccount);

            send(tuple, values);

        }catch (Exception e){

            LOG.error("Error when processing Linked Accounts. Tuple: {}, Message: {}, Error: {}", tuple, e.getMessage(), e);
            error(e, tuple);
        }
    }
}
