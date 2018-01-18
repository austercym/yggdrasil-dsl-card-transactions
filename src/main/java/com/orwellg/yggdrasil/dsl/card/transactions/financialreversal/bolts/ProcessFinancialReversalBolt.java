package com.orwellg.yggdrasil.dsl.card.transactions.financialreversal.bolts;

import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
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
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA));
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

            LOG.info("{}Financial Reversal processing...", logPrefix);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_DATA, eventData);
            send(input, values);
        } catch (Exception e) {
            LOG.error("{}Financial Reversal processing error. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }
}
