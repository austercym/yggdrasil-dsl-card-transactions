package com.yggdrasil.dsl.card.transactions.topology.bolts.processors.presentment;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.LinkedAccount;
import com.yggdrasil.dsl.card.transactions.GpsMessage;
import com.yggdrasil.dsl.card.transactions.GpsMessageException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class PresentmentOfflineTransactionBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(PresentmentOfflineTransactionBolt.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "gpsMessage"));
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.info("Processing offline transaction. MessageID: {}", tuple.getMessageId());

        try {
            String key = tuple.getStringByField("key");
            String processId = tuple.getStringByField("processId");
            Message eventData = (Message) tuple.getValueByField("eventData");
            GpsMessage presentment = (GpsMessage) tuple.getValueByField("gpsMessage");
            List<LinkedAccount> linkedAccounts = (List<LinkedAccount>) tuple.getValueByField("retrieveValue");

            LOG.info("Processing linked accounts for message with key: {}, processId: {}, cardTransactionId: {}, transaction date: {}", key, processId,
                    eventData.getCustRef(), eventData.getPOSTimeDE12());


            Optional<LinkedAccount> maxDateOptional = linkedAccounts.stream()
                    .max(Comparator.comparing(LinkedAccount::getFromTimestamp));

            if (!maxDateOptional.isPresent()){
                throw new GpsMessageException(String.format("No linked accounts found for cardId: {}, transaction date time: {}. Message key: {}. processId: {}",
                        presentment.getDebitCardId(), presentment.getTransactionTimestamp(), key, processId));
            }

            LinkedAccount linkedAccount = maxDateOptional.get();
            LOG.info("Selected Linked Account for message key: {}, processId: {}. LinkedAccountId: {}, FromTimestamp: {}", key, processId,
                    linkedAccount.getInternalAccountId(), linkedAccount.getFromTimestamp());


            //2. update Message object with account id data
            presentment.setInternalAccountId(linkedAccount.getInternalAccountId());
            presentment.setInternalAccountCurrency(linkedAccount.getInternalAccountCurrency());

            //3. send message to further processing
            Map<String, Object> values = new HashMap<>();
            values.put("key", key);
            values.put("processId", processId);
            values.put("eventData", eventData);
            values.put("gpsMessage", presentment);

            LOG.info(". GpsTransactionId: {}, GpsTransactionLink: {}, key: {}, processId: {}", eventData.getTXnID(), eventData.getTransLink(), key, processId);
            send(tuple, values);

        }catch (Exception e){
            LOG.error("Error when processing Linked Accounts. Tuple: {}, Message: {}, Error: {}", tuple, e.getMessage(), e);
            error(e, tuple);
        }


    }
}
