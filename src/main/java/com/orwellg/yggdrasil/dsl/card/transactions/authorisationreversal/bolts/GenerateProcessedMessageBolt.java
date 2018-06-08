package com.orwellg.yggdrasil.dsl.card.transactions.authorisationreversal.bolts;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.yggdrasil.card.transaction.commons.DuplicateChecker;
import com.orwellg.yggdrasil.card.transaction.commons.MessageProcessedFactory;
import com.orwellg.yggdrasil.card.transaction.commons.model.TransactionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GenerateProcessedMessageBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(GenerateProcessedMessageBolt.class);

    private DuplicateChecker duplicateChecker;

    void setDuplicateChecker(DuplicateChecker duplicateChecker) {
        this.duplicateChecker = duplicateChecker;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        setDuplicateChecker(new DuplicateChecker());
    }

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
            boolean isDuplicate = duplicateChecker.isDuplicate(event, transactionList);

            if (isDuplicate) {
                LOG.info(
                        "{}Processing duplicated message. ProviderMessageId: {}",
                        logPrefix, event.getProviderMessageId());
            } else {
                if (event.getSettlementAmount().compareTo(lastTransaction.getEarmarkAmount().abs()) > 0) {
                    throw new IllegalArgumentException("Authorisation reversal amount is greater than earmarked amount");
                }
            }

            MessageProcessed processedMessage = isDuplicate
                    ? MessageProcessedFactory.from(event, lastTransaction)
                    : generateMessageProcessed(event, lastTransaction, logPrefix);

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

        MessageProcessed messageProcessed = MessageProcessedFactory.from(transactionInfo);

        messageProcessed.setEarmarkAmount(DecimalTypeUtils.toDecimal(transactionInfo.getSettlementAmount()));
        messageProcessed.setEarmarkCurrency(lastTransaction.getInternalAccountCurrency());

        messageProcessed.setTotalClientAmount(DecimalTypeUtils.toDecimal(lastTransaction.getClientAmount()));
        messageProcessed.setTotalClientCurrency(lastTransaction.getClientCurrency());
        messageProcessed.setTotalEarmarkAmount(DecimalTypeUtils.toDecimal(
                lastTransaction.getEarmarkAmount().add(transactionInfo.getSettlementAmount())));
        messageProcessed.setTotalEarmarkCurrency(lastTransaction.getInternalAccountCurrency());
        messageProcessed.setTotalWirecardAmount(DecimalTypeUtils.toDecimal(lastTransaction.getWirecardAmount()));
        messageProcessed.setTotalWirecardCurrency(transactionInfo.getSettlementCurrency());

        messageProcessed.setInternalAccountCurrency(lastTransaction.getInternalAccountCurrency());
        messageProcessed.setInternalAccountId(lastTransaction.getInternalAccountId());

        LOG.debug("{}GPS message processed generated: {}", logPrefix, messageProcessed);
        return messageProcessed;
    }
}
