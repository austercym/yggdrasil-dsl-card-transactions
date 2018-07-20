package com.orwellg.yggdrasil.dsl.card.transactions.externallyauthorised.bolts;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.yggdrasil.card.transaction.commons.DuplicateChecker;
import com.orwellg.yggdrasil.card.transaction.commons.MessageProcessedFactory;
import com.orwellg.yggdrasil.card.transaction.commons.bolts.Fields;
import com.orwellg.yggdrasil.card.transaction.commons.model.TransactionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.math.BigDecimal;
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
        String logPrefix = null;

        try {
            String key = input.getStringByField(Fields.KEY);
            String processId = input.getStringByField(Fields.PROCESS_ID);
            logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

            LOG.info("{} Generating processed message", logPrefix);

            TransactionInfo event = (TransactionInfo) input.getValueByField(Fields.EVENT_DATA);
            List<CardTransaction> transactionList = (List<CardTransaction>) input.getValueByField(Fields.TRANSACTION_LIST);

            if (!"I".equalsIgnoreCase(event.getMessage().getTxnStatCode())) {
                throw new Exception("Authorisation has not been declined externally");
            }

            MessageProcessed result;
            if (transactionList == null || transactionList.isEmpty()) {
                LOG.info("{}No matching authorisation - no account balance changes required", logPrefix);
                result = MessageProcessedFactory.from(event);
            } else if (duplicateChecker.isDuplicate(event, transactionList)) {
                LOG.info(
                        "{}Processing duplicated message. ProviderMessageId: {}",
                        logPrefix, event.getProviderMessageId());
                result = MessageProcessedFactory.from(event, transactionList.get(0));
            } else {
                CardTransaction previousTransaction = transactionList.get(0);
                LOG.info(
                        "{}Previous transaction: Type={}, EarmarkedAmount={} {}, ProviderMessageId={}",
                        logPrefix, previousTransaction.getMessageType(),
                        previousTransaction.getEarmarkAmount(), previousTransaction.getEarmarkCurrency(),
                        previousTransaction.getProviderMessageId());
                if (BigDecimal.ZERO.compareTo(previousTransaction.getEarmarkAmount()) == 0) {
                    LOG.info(
                            "{}No earmark placed for the previous authorisation - no account balance changes required",
                            logPrefix);
                    result = MessageProcessedFactory.from(event);
                } else if (event.getSettlementAmount().compareTo(previousTransaction.getEarmarkAmount()) != 0) {
                    throw new IllegalArgumentException("Settlement amount is different than earmarked amount");
                } else {
                    LOG.info("{}Reverting previous authorisation", logPrefix);
                    result = revertPreviousAuthorisation(event, previousTransaction);
                }
            }

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_NAME, CardTransactionEvents.RESPONSE_MESSAGE.getEventName());
            values.put(Fields.RESULT, result);
            send(input, values);
        } catch (Exception e) {
            LOG.error("{}Error generating processed message. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    private MessageProcessed revertPreviousAuthorisation(
            TransactionInfo transactionInfo, CardTransaction lastTransaction) {

        MessageProcessed messageProcessed = MessageProcessedFactory.from(transactionInfo);

        messageProcessed.setEarmarkAmount(DecimalTypeUtils.toDecimal(transactionInfo.getSettlementAmount().abs()));
        messageProcessed.setEarmarkCurrency(lastTransaction.getInternalAccountCurrency());

        messageProcessed.setTotalClientAmount(DecimalTypeUtils.toDecimal(lastTransaction.getClientAmount()));
        messageProcessed.setTotalClientCurrency(lastTransaction.getClientCurrency());
        messageProcessed.setTotalEarmarkAmount(DecimalTypeUtils.toDecimal(
                lastTransaction.getEarmarkAmount().add(transactionInfo.getSettlementAmount().abs())));
        messageProcessed.setTotalEarmarkCurrency(lastTransaction.getInternalAccountCurrency());
        messageProcessed.setTotalWirecardAmount(DecimalTypeUtils.toDecimal(lastTransaction.getWirecardAmount()));
        messageProcessed.setTotalWirecardCurrency(transactionInfo.getSettlementCurrency());

        messageProcessed.setInternalAccountCurrency(lastTransaction.getInternalAccountCurrency());
        messageProcessed.setInternalAccountId(lastTransaction.getInternalAccountId());

        messageProcessed.setRequest(transactionInfo.getMessage());
        return messageProcessed;
    }
}
