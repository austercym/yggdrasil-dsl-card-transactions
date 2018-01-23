package com.orwellg.yggdrasil.dsl.card.transactions.savetoscylla.bolts;

import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class PrepareDataBolt extends com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaEventProcessBolt {

    private static final Logger LOG = LogManager.getLogger(PrepareDataBolt.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(Fields.KEY, Fields.PROCESS_ID, Fields.TRANSACTION));
    }

    @Override
    public void sendNextStep(Tuple input, Event event) {

        try {
            String key = event.getEvent().getKey();
            String processId = event.getProcessIdentifier().getUuid();

            LOG.info("Key: {} | ProcessId: {} | Received GPS message event", key, processId);

            GpsMessageProcessed message = gson.fromJson(event.getEvent().getData(), GpsMessageProcessed.class);
            CardTransaction transaction = new CardTransaction();
            transaction.setGpsTransactionLink(message.getGpsTransactionLink());
            transaction.setGpsTransactionId(message.getGpsTransactionId());
            transaction.setGpsTransactionDateTime(Instant.ofEpochMilli(message.getGpsTransactionTime()));
            transaction.setTransactionTimestamp(Instant.ofEpochMilli(message.getTransactionTimestamp()));
            transaction.setGpsMessageType(message.getGpsMessageType());
            transaction.setDebitCardId(message.getDebitCardId());
            transaction.setInternalAccountId(message.getInternalAccountId());
            transaction.setInternalAccountCurrency(message.getInternalAccountCurrency());
            transaction.setWirecardAmount(message.getAppliedWirecardAmount().getValue());
            transaction.setWirecardCurrency(message.getAppliedWirecardCurrency());
            transaction.setClientAmount(message.getAppliedClientAmount().getValue());
            transaction.setClientCurrency(message.getAppliedClientCurrency());
            transaction.setEarmarkAmount(message.getAppliedEarmarkAmount().getValue());
            transaction.setEarmarkCurrency(message.getAppliedEarmarkCurrency());
            transaction.setFeeAmount(message.getAppliedFeesAmount().getValue());
            transaction.setFeeCurrency(message.getAppliedFeesCurrency());

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.TRANSACTION, transaction);
            send(input, values);

            LOG.info("[Key: {}][ProcessId: {}]: Card transaction object prepared to save in Scylla.", key, processId);

        }catch(Exception e){
            LOG.error("Error occurred when preparing card transaction object to save in Scylla. Message: {}", input, e.getMessage(), e);
            error(e, input);
        }
    }
}
