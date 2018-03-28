package com.orwellg.yggdrasil.dsl.card.transactions.authorisationreversal.bolts;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.yggdrasil.card.transaction.commons.model.TransactionInfo;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.MessageProcessedFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenerateProcessedMessageBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(GenerateProcessedMessageBolt.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_NAME, Fields.RESULT));
    }

    @Override
    public void execute(Tuple input) {
        String key = input.getStringByField(Fields.KEY);
        String processId = input.getStringByField(Fields.PROCESS_ID);
        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        LOG.info("{} Generating processed message", logPrefix);

        try {
            TransactionInfo event = (TransactionInfo) input.getValueByField(Fields.EVENT_DATA);
            List<CardTransaction> transactionList = (List<CardTransaction>) input.getValueByField(Fields.TRANSACTION_LIST);

            if (transactionList == null || transactionList.isEmpty()){
                throw new IllegalArgumentException("Empty transaction list - cannot process authorisation reversal");
            }
            CardTransaction lastTransaction = transactionList.get(0);
            if (event.getSettlementAmount().compareTo(lastTransaction.getEarmarkAmount().abs()) > 0) {
                throw new IllegalArgumentException("Authorisation reversal amount is greater than earmarked amount");
            }
            if (event.getSettlementAmount().compareTo(lastTransaction.getWirecardAmount().abs()) < 0) {
                throw new IllegalArgumentException("Authorisation reversal amount is greater than amount sent to Wirecard");
            }

            MessageProcessed processedMessage = generateMessageProcessed(event, lastTransaction, logPrefix);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_NAME, CardTransactionEvents.RESPONSE_MESSAGE.getEventName());
            values.put(Fields.RESULT, processedMessage);
            send(input, values);
        } catch (Exception e) {
            LOG.error("{}Error generating processed message. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    private MessageProcessed generateMessageProcessed(
            TransactionInfo transactionInfo, CardTransaction lastTransaction,
            String logPrefix) {

        LOG.debug("{}Generating GPS message processed", logPrefix);

        MessageProcessed MessageProcessed = MessageProcessedFactory.from(transactionInfo);

        MessageProcessed.setEarmarkAmount(DecimalTypeUtils.toDecimal(transactionInfo.getSettlementAmount()));
        MessageProcessed.setEarmarkCurrency(lastTransaction.getInternalAccountCurrency());
        MessageProcessed.setWirecardAmount(DecimalTypeUtils.toDecimal(transactionInfo.getSettlementAmount().negate()));
        MessageProcessed.setWirecardCurrency(transactionInfo.getSettlementCurrency());

        MessageProcessed.setTotalEarmarkAmount(DecimalTypeUtils.toDecimal(
                lastTransaction.getEarmarkAmount().add(transactionInfo.getSettlementAmount())));
        MessageProcessed.setTotalEarmarkCurrency(lastTransaction.getInternalAccountCurrency());
        MessageProcessed.setTotalWirecardAmount(DecimalTypeUtils.toDecimal(
                lastTransaction.getWirecardAmount().subtract(transactionInfo.getSettlementAmount())));
        MessageProcessed.setTotalWirecardCurrency(transactionInfo.getSettlementCurrency());

        MessageProcessed.setInternalAccountCurrency(lastTransaction.getInternalAccountCurrency());
        MessageProcessed.setInternalAccountId(lastTransaction.getInternalAccountId());

        LOG.debug("{}GPS message processed generated: {}", logPrefix, MessageProcessed);
        return MessageProcessed;
    }
}
