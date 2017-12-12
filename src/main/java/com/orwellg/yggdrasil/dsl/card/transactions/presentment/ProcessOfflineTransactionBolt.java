package com.orwellg.yggdrasil.dsl.card.transactions.presentment;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.LinkedAccount;
import com.orwellg.yggdrasil.dsl.card.transactions.model.GpsMessageProcessingException;
import com.orwellg.yggdrasil.dsl.card.transactions.model.PresentmentErrorCode;
import com.orwellg.yggdrasil.dsl.card.transactions.model.PresentmentMessage;
import com.orwellg.yggdrasil.dsl.card.transactions.presentment.services.AuthorisationValidationService;
import com.orwellg.yggdrasil.dsl.card.transactions.presentment.services.LinkedAccountValidationService;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class ProcessOfflineTransactionBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(ProcessOfflineTransactionBolt.class);
    private LinkedAccountValidationService linkedAccountService;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        linkedAccountService = new LinkedAccountValidationService();
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "gpsMessage"));
    }

    @Override
    public void execute(Tuple tuple) {

        try {
            String key = tuple.getStringByField("key");
            String processId = tuple.getStringByField("processId");
            Message eventData = (Message) tuple.getValueByField("eventData");

            LOG.debug("Key: {} | ProcessId: {} | Processing offline transaction", key, processId);

            PresentmentMessage presentment = (PresentmentMessage) tuple.getValueByField("gpsMessage");
            List<LinkedAccount> linkedAccounts = (List<LinkedAccount>) tuple.getValueByField("retrieveValue");

            LOG.debug("Key: {} | ProcessId: {} | " + "Selecting linked account. CardTransactionId: {}, Transaction date: {}", key, processId,
                    eventData.getCustRef(), eventData.getPOSTimeDE12());


            LinkedAccount linkedAccount = linkedAccountService.getLast(linkedAccounts);

            if (linkedAccount == null){
                throw new GpsMessageProcessingException(PresentmentErrorCode.LINKED_ACCOUNT_MISSING, "No linked accounts found for cardId: "+ presentment.getDebitCardId() +  " transaction date time: " + presentment.getTransactionTimestamp());
            }

            LOG.debug("Key: {} | ProcessId: {} | Selected Linked Account. LinkedAccountId: {}, FromTimestamp: {}", key, processId,
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

            send(tuple, values);

        }catch (Exception e){

            LOG.error("Error when processing Linked Accounts. Tuple: {}, Message: {}, Error: {}", tuple, e.getMessage(), e);
            error(e, tuple);
        }


    }
}
