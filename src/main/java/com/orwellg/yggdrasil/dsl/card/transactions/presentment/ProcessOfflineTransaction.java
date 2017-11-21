package com.orwellg.yggdrasil.dsl.card.transactions.presentment;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.LinkedAccount;
import com.orwellg.yggdrasil.dsl.card.transactions.GpsMessageException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class ProcessOfflineTransaction extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(ProcessOfflineTransaction.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "gpsMessage"));
        addFielsDefinition(CardPresentmentDSLTopology.ERROR_STREAM, Arrays.asList("key", "processId", "eventData", "exceptionMessage", "exceptionStackTrace"));
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.info("Processing offline transaction. MessageID: {}", tuple.getMessageId());

        try {
            String key = tuple.getStringByField("key");
            String processId = tuple.getStringByField("processId");
            Message eventData = (Message) tuple.getValueByField("eventData");
            PresentmentMessage presentment = (PresentmentMessage) tuple.getValueByField("gpsMessage");
            List<LinkedAccount> linkedAccounts = (List<LinkedAccount>) tuple.getValueByField("retrieveValue");

            LOG.info("Processing linked accounts for message with key: {}, processId: {}, cardTransactionId: {}, transaction date: {}", key, processId,
                    eventData.getCustRef(), eventData.getPOSTimeDE12());


            Optional<LinkedAccount> maxDateOptional = linkedAccounts.stream()
                    .max(Comparator.comparing(LinkedAccount::getFromTimestamp));

            if (!maxDateOptional.isPresent()){
                throw new GpsMessageException("No linked accounts found for cardId: " + presentment.getDebitCardId() + " transaction date time: " + presentment.getTransactionTimestamp()
                        + ", Message key: " + key + ", processId: " + processId);
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

            Map<String, Object> values = new HashMap<>();
            values.put("key", tuple.getValueByField("key"));
            values.put("processId", tuple.getValueByField("processId"));
            values.put("eventData", tuple.getValueByField("eventData"));
            values.put("exceptionMessage", ExceptionUtils.getMessage(e));
            values.put("exceptionStackTrace", ExceptionUtils.getStackTrace(e));

            send(CardPresentmentDSLTopology.ERROR_STREAM, tuple, values);
            LOG.info("Error when processing Linked Accounts - error send to corresponded kafka topic. Tuple: {}, Message: {}, Error: {}", tuple, e.getMessage(), e);
        }


    }
}
