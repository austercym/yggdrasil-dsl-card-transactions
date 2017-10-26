package com.yggdrasil.dsl.card.transactions.topology.bolts.event;

import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CardSaveGpsMessageProcessedBolt extends com.orwellg.umbrella.commons.storm.topology.component.bolt.KafkaEventProcessBolt {

    private static final Logger LOG = LogManager.getLogger(CardSaveGpsMessageProcessedBolt.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("gpsTransactionLink", "gpsTransactionId", "debitCardId", "transactionTimestamp",
                "internalAccountId", "wirecardAmount", "wirecardCurrency", "blockedClientAmount", "blockedClientCurrency"));
    }

    @Override
    public void sendNextStep(Tuple input, Event event) {

        try {
            String key = event.getEvent().getKey();
            String processId = event.getProcessIdentifier().getUuid();

            LOG.info("[Key: {}][ProcessId: {}]: Received GPS message event", key, processId);

            GpsMessageProcessed message = gson.fromJson(event.getEvent().getData(), GpsMessageProcessed.class);

            Map<String, Object> values = new HashMap<>();

            values.put("gpsTransactionLink", message.getGpsTransactionLink());
            values.put("gpsTransactionId", message.getGpsTransactionId());
            values.put("debitCardId", message.getDebitCardId());

            //todo: avro - send date or timestamp in long?
            //SimpleDateFormat parser = new SimpleDateFormat("YYYY-MM-dd hh:mm:ss.SSS");
            //String timestamp = message.getTransactionTimestamp();
            //Date date = parser.parse(timestamp);
            values.put("transactionTimestamp", new Date()); //todo!!

            values.put("internalAccountId", message.getInternalAccountId());

            //todo: avro send bigDecimal??
            NumberFormat formatter = new DecimalFormat("#0.00");
            values.put("wirecardAmount", new BigDecimal(formatter.format(message.getWirecardAmount())));
            values.put("blockedClientAmount", new BigDecimal(formatter.format(message.getBlockedClientAmount())));

            values.put("wirecardCurrency", message.getWirecardCurrency());
            values.put("blockedClientCurrency", message.getWirecardCurrency());

            send(input, values);

            LOG.info("[Key: {}][ProcessId: {}]: GPS message event sent.", key, processId);

        }catch(Exception e){

        }
    }

}
