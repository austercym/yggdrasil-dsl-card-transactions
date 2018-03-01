package com.orwellg.yggdrasil.dsl.card.transactions.common.bolts;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.MessageProcessedFactory;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenericMessageProcessingBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(GenericMessageProcessingBolt.class);

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

            LOG.debug("{}Processing", logPrefix);
            MessageProcessed result = MessageProcessedFactory.from(eventData);

            if (transactionList == null || transactionList.isEmpty()) {
                throw new IllegalArgumentException("Empty transaction list - cannot process");
            }

            CardTransaction lastTransaction = transactionList.get(0);
            if (isDuplicatedMessage(eventData, transactionList)) {
                LOG.info("{}Ignore message. It has been already processed.", logPrefix);
                copyValuesFromLatestTransaction(eventData, result, lastTransaction);
            } else {
                calculateNewValues(eventData, result, lastTransaction);
            }

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_NAME, CardTransactionEvents.RESPONSE_MESSAGE.getEventName());
            values.put(Fields.RESULT, result);
            send(input, values);
        } catch (Exception e) {
            LOG.error("{}Processing error. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    private void calculateNewValues(TransactionInfo eventData, MessageProcessed result, CardTransaction lastTransaction) {
        result.setWirecardAmount(DecimalTypeUtils.toDecimal(eventData.getSettlementAmount().negate()));
        result.setWirecardCurrency(eventData.getSettlementCurrency());
        result.setClientAmount(DecimalTypeUtils.toDecimal(eventData.getSettlementAmount()));
        result.setClientCurrency(lastTransaction.getInternalAccountCurrency());

        result.setTotalWirecardAmount(DecimalTypeUtils.toDecimal(
                ObjectUtils.firstNonNull(lastTransaction.getWirecardAmount(), BigDecimal.ZERO)
                        .subtract(eventData.getSettlementAmount())));
        result.setTotalWirecardCurrency(eventData.getSettlementCurrency());
        result.setTotalClientAmount(DecimalTypeUtils.toDecimal(
                ObjectUtils.firstNonNull(lastTransaction.getClientAmount(), BigDecimal.ZERO)
                        .add(eventData.getSettlementAmount())));
        result.setTotalClientCurrency(lastTransaction.getInternalAccountCurrency());
        result.setTotalEarmarkAmount(DecimalTypeUtils.toDecimal(
                ObjectUtils.firstNonNull(lastTransaction.getEarmarkAmount(), BigDecimal.ZERO)));
        result.setTotalEarmarkCurrency(lastTransaction.getInternalAccountCurrency());
    }

    private void copyValuesFromLatestTransaction(TransactionInfo eventData, MessageProcessed result, CardTransaction lastTransaction) {
        result.setWirecardAmount(DecimalTypeUtils.toDecimal(BigDecimal.ZERO));
        result.setWirecardCurrency(eventData.getSettlementCurrency());
        result.setClientAmount(DecimalTypeUtils.toDecimal(BigDecimal.ZERO));
        result.setClientCurrency(lastTransaction.getInternalAccountCurrency());

        result.setTotalWirecardAmount(DecimalTypeUtils.toDecimal(
                ObjectUtils.firstNonNull(lastTransaction.getWirecardAmount(), BigDecimal.ZERO)));
        result.setTotalWirecardCurrency(lastTransaction.getWirecardCurrency());
        result.setTotalClientAmount(DecimalTypeUtils.toDecimal(
                ObjectUtils.firstNonNull(lastTransaction.getClientAmount(), BigDecimal.ZERO)));
        result.setTotalClientCurrency(lastTransaction.getClientCurrency());
        result.setTotalEarmarkAmount(DecimalTypeUtils.toDecimal(
                ObjectUtils.firstNonNull(lastTransaction.getEarmarkAmount(), BigDecimal.ZERO)));
        result.setTotalEarmarkCurrency(lastTransaction.getEarmarkCurrency());
    }

    private boolean isDuplicatedMessage(TransactionInfo eventData, List<CardTransaction> transactionList) {
        return transactionList.stream().anyMatch(
                t -> eventData.getProviderMessageId().equals(t.getProviderMessageId()));
    }
}
