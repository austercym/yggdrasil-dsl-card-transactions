package com.orwellg.yggdrasil.dsl.card.transactions.saveToScylla;

import com.orwellg.umbrella.avro.types.commons.Decimal;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import org.apache.avro.LogicalTypes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.math.BigDecimal;
import java.util.*;

public class CardSaveGpsMessageProcessedBolt extends com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaEventProcessBolt {

    private static final Logger LOG = LogManager.getLogger(CardSaveGpsMessageProcessedBolt.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("gpsTransactionLink", "gpsTransactionId", "gpsTransactionDateTime", "debitCardId", "transactionTimestamp",
                "internalAccountId", "wirecardAmount", "wirecardCurrency", "blockedClientAmount", "blockedClientCurrency", "gpsMessageType", "feeAmount", "feeCurrency", "internalAccountCurrency"));
    }

    @Override
    public void sendNextStep(Tuple input, Event event) {

        try {
            String key = event.getEvent().getKey();
            String processId = event.getProcessIdentifier().getUuid();

            LOG.info("[Key: {}][ProcessId: {}]: Received GPS message event", key, processId);

            GpsMessageProcessed message = gson.fromJson(event.getEvent().getData(), GpsMessageProcessed.class);
            Decimal wirecardAmount =  message.getWirecardAmount();
            Decimal clientAmount = message.getBlockedClientAmount();
            Decimal feeAmount = message.getFeesAmount();

            Date transactionTimestamp = new Date(message.getTransactionTimestamp()*1000);
            Date gpsDate = new Date(message.getGpsTransactionTime()*1000);
            //todo: avro - send date or timestamp in long?
            //todo: avro send bigDecimal??

            Map<String, Object> values = new HashMap<>();

            values.put("gpsTransactionLink", message.getGpsTransactionLink());
            values.put("gpsTransactionId", message.getGpsTransactionId());
            values.put("debitCardId", message.getDebitCardId());
            values.put("transactionTimestamp", transactionTimestamp);
            values.put("gpsTransactionDateTime", gpsDate);
            values.put("internalAccountId", message.getInternalAccountId());
            values.put("wirecardAmount", (wirecardAmount == null) ? 0 : wirecardAmount.getValue());
            values.put("blockedClientAmount", (clientAmount == null) ? 0 : clientAmount.getValue());
            values.put("wirecardCurrency", message.getWirecardCurrency());
            values.put("blockedClientCurrency", message.getBlockedClientCurrency());
            values.put("feeAmount", (feeAmount == null) ? 0 : feeAmount.getValue());
            values.put("feeCurrency", message.getFeesCurrency());
            values.put("gpsMessageType", message.getGpsMessageType());
            values.put("internalAccountCurrency", message.getInternalAccountCurrency());

            send(input, values);

            LOG.info("[Key: {}][ProcessId: {}]: GPS message event sent.", key, processId);

        }catch(Exception e){
            LOG.error("The received event {} can not be decoded. Message: {}", input, e.getMessage(), e);
            error(e, input);
        }
    }

}
