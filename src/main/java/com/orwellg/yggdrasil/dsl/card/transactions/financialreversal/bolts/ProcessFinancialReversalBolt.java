package com.orwellg.yggdrasil.dsl.card.transactions.financialreversal.bolts;

import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.GpsMessageProcessedFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProcessFinancialReversalBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(ProcessFinancialReversalBolt.class);

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
            List<CardTransaction> transactionList = (List<CardTransaction>) input.getValueByField(Fields.TRANSACTION_LIST);
            logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

            LOG.debug("{}Financial Reversal processing", logPrefix);
            GpsMessageProcessed result = GpsMessageProcessedFactory.from(eventData);

            if (transactionList == null || transactionList.isEmpty()){
                throw new IllegalArgumentException("Empty transaction list - cannot process financial reversal");
            }
            CardTransaction lastTransaction = transactionList.get(0);

            // TODO: Validate returned amounts
            // TODO: Add total/applied amounts
            result.setWirecardAmount(DecimalTypeUtils.toDecimal(eventData.getSettlementAmount().negate()));
            result.setWirecardCurrency(eventData.getSettlementCurrency());
            result.setClientAmount(DecimalTypeUtils.toDecimal(eventData.getSettlementAmount()));
            result.setClientCurrency(lastTransaction.getInternalAccountCurrency());

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_NAME, CardTransactionEvents.RESPONSE_MESSAGE.getEventName());
            values.put(Fields.RESULT, result);
            send(input, values);
        } catch (Exception e) {
            LOG.error("{}Financial Reversal processing error. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }
}
