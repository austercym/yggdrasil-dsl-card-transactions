package com.orwellg.yggdrasil.dsl.card.transactions.common.bolts;

import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.avro.types.gps.ResponseMsg;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class GpsMessageProcessedGeneratorBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(GpsMessageProcessedGeneratorBolt.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_NAME, Fields.RESULT));
    }

    @Override
    public void execute(Tuple input) {
        String logPrefix = null;
        try {
            String key = input.getStringByField(Fields.KEY);
            String processId = input.getStringByField(Fields.PROCESS_ID);
            TransactionInfo eventData = (TransactionInfo) input.getValueByField(Fields.EVENT_DATA);
            logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

            GpsMessageProcessed response = generateMessageProcessed(eventData, logPrefix);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_NAME, CardTransactionEvents.RESPONSE_MESSAGE.getEventName());
            values.put(Fields.RESULT, response);
            send(input, values);
        } catch (Exception e) {
            LOG.error("{}GPS message processed generation error. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    private GpsMessageProcessed generateMessageProcessed(TransactionInfo transactionInfo, String logPrefix) {

        LOG.debug("{}Generating gpsMessageProcessed message", logPrefix);

        GpsMessageProcessed gpsMessageProcessed = new GpsMessageProcessed();
        gpsMessageProcessed.setGpsMessageType(transactionInfo.getMessage().getTxnType());
        gpsMessageProcessed.setGpsTransactionLink(transactionInfo.getGpsTransactionLink());
        gpsMessageProcessed.setGpsTransactionId(transactionInfo.getGpsTransactionId());
        gpsMessageProcessed.setDebitCardId(transactionInfo.getDebitCardId());

        gpsMessageProcessed.setSpendGroup(transactionInfo.getSpendGroup());
        gpsMessageProcessed.setTransactionTimestamp(new Date().getTime());

        // TODO: temp values below
        gpsMessageProcessed.setBlockedClientAmount(DecimalTypeUtils.toDecimal(42));
        gpsMessageProcessed.setBlockedClientCurrency("FOO");
        gpsMessageProcessed.setWirecardAmount(DecimalTypeUtils.toDecimal(42));
        gpsMessageProcessed.setWirecardCurrency("FOO");
        gpsMessageProcessed.setFeesAmount(DecimalTypeUtils.toDecimal(0));
        gpsMessageProcessed.setFeesCurrency("FOO");

        gpsMessageProcessed.setAppliedBlockedClientAmount(DecimalTypeUtils.toDecimal(0));
        gpsMessageProcessed.setAppliedBlockedClientCurrency("FOO");
        gpsMessageProcessed.setAppliedWirecardAmount(DecimalTypeUtils.toDecimal(0));
        gpsMessageProcessed.setAppliedWirecardCurrency("FOO");

        gpsMessageProcessed.setInternalAccountCurrency("FOO");
        gpsMessageProcessed.setInternalAccountId(42L);

        gpsMessageProcessed.setAppliedFeesCurrency("FOO");
        gpsMessageProcessed.setMessageType("FOO");

        ResponseMsg responseMsg = new ResponseMsg();
        gpsMessageProcessed.setEhiResponse(responseMsg);

        LOG.debug("{}Response message generated: {}", logPrefix, gpsMessageProcessed);
        return gpsMessageProcessed;
    }
}
