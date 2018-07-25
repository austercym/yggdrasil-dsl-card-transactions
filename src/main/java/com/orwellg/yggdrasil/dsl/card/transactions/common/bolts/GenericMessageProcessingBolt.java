package com.orwellg.yggdrasil.dsl.card.transactions.common.bolts;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.avro.types.cards.MessageType;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.yggdrasil.card.transaction.commons.DuplicateChecker;
import com.orwellg.yggdrasil.card.transaction.commons.MessageProcessedBuilder;
import com.orwellg.yggdrasil.card.transaction.commons.bolts.Fields;
import com.orwellg.yggdrasil.card.transaction.commons.model.TransactionInfo;
import com.orwellg.yggdrasil.card.transaction.commons.validation.TransactionOrderValidator;
import com.orwellg.yggdrasil.card.transaction.commons.validation.TransactionOrderValidatorFactory;
import org.apache.commons.lang3.ObjectUtils;
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

public class GenericMessageProcessingBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(GenericMessageProcessingBolt.class);

    private DuplicateChecker duplicateChecker;
    private TransactionOrderValidator orderValidator;
    private MessageType messageType;

    public GenericMessageProcessingBolt(MessageType messageType) {
        this.messageType = messageType;
    }

    void setDuplicateChecker(DuplicateChecker duplicateChecker) {
        this.duplicateChecker = duplicateChecker;
    }

    void setOrderValidator(TransactionOrderValidator orderValidator) {
        this.orderValidator = orderValidator;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        setDuplicateChecker(new DuplicateChecker());
        setOrderValidator(TransactionOrderValidatorFactory.get(messageType));
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
            TransactionInfo eventData = (TransactionInfo) input.getValueByField(Fields.EVENT_DATA);
            List<CardTransaction> transactionList = (List<CardTransaction>) input.getValueByField(Fields.TRANSACTION_LIST);
            logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

            LOG.debug("{}Processing", logPrefix);
            MessageProcessed result;

            if (transactionList == null || transactionList.isEmpty()) {
                throw new IllegalArgumentException("Empty transaction list - cannot process");
            }

            CardTransaction lastTransaction = transactionList.get(0);
            if (duplicateChecker.isDuplicate(eventData, transactionList)) {
                LOG.info("{}Ignore message. It has been already processed.", logPrefix);
                result = MessageProcessedBuilder.from(eventData, lastTransaction);
            } else {
                orderValidator.validate(transactionList).throwIfInvalid();

                result = MessageProcessedBuilder.from(eventData);
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

        result.setInternalAccountId(lastTransaction.getInternalAccountId());
        result.setInternalAccountCurrency(lastTransaction.getInternalAccountCurrency());
    }
}
