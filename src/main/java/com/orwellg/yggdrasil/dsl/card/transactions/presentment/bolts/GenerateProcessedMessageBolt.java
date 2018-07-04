package com.orwellg.yggdrasil.dsl.card.transactions.presentment.bolts;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.LinkedAccount;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.yggdrasil.card.transaction.commons.DuplicateChecker;
import com.orwellg.yggdrasil.card.transaction.commons.MessageProcessedFactory;
import com.orwellg.yggdrasil.card.transaction.commons.model.TransactionInfo;
import org.apache.commons.collections4.CollectionUtils;
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
        addFielsDefinition(Arrays.asList(Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_NAME, Fields.RESULT));
    }

    @Override
    public void execute(Tuple tuple) {

        String key = (String) tuple.getValueByField(Fields.KEY);
        String originalProcessId = (String) tuple.getValueByField(Fields.PROCESS_ID);
        TransactionInfo eventData = (TransactionInfo) tuple.getValueByField(Fields.EVENT_DATA);

        LOG.debug("Key: {} | ProcessId: {} | Preparing Response. ProviderMessageId: {}, ProviderTransactionId: {}", key, originalProcessId, eventData.getProviderMessageId(),
                eventData.getProviderTransactionId());

        try {

            TransactionInfo presentment = (TransactionInfo) tuple.getValueByField(Fields.EVENT_DATA);
            List<CardTransaction> transactionHistory = tuple.contains(Fields.TRANSACTION_LIST)
                    ? (List<CardTransaction>) tuple.getValueByField(Fields.TRANSACTION_LIST)
                    : null;
            LinkedAccount linkedAccount = tuple.contains(Fields.LINKED_ACCOUNT)
                    ? (LinkedAccount) tuple.getValueByField(Fields.LINKED_ACCOUNT)
                    : null;

            BigDecimal lastEarmarkAmount;
            BigDecimal earmarkAmount;
            BigDecimal lastClientAmount;
            BigDecimal clientAmount = presentment.getSettlementAmount();
            BigDecimal lastWirecardAmount;
            BigDecimal wirecardAmount = presentment.getSettlementAmount().negate();
            String clientAccountCurrency;
            String wirecardAccountCurrency = presentment.getSettlementCurrency();
            String clientAccountId;
            CardTransaction lastTransaction = CollectionUtils.isEmpty(transactionHistory)
                    ? null : transactionHistory.get(0);

            MessageProcessed messageProcessed;

            if (duplicateChecker.isDuplicate(presentment, transactionHistory)) {
                LOG.info(
                        " Key: {} | ProcessId: {} | Processing duplicated message. ProviderMessageId: {}",
                        key, originalProcessId, presentment.getProviderMessageId());
                messageProcessed = MessageProcessedFactory.from(presentment, lastTransaction);
            } else {
                if (lastTransaction != null) {
                    lastEarmarkAmount = ObjectUtils.firstNonNull(lastTransaction.getEarmarkAmount(), BigDecimal.ZERO);
                    earmarkAmount = lastEarmarkAmount.abs();
                    lastClientAmount = lastTransaction.getClientAmount();
                    lastWirecardAmount = lastTransaction.getWirecardAmount();
                    clientAccountCurrency = lastTransaction.getInternalAccountCurrency();
                    clientAccountId = lastTransaction.getInternalAccountId();
                } else if (linkedAccount != null) {
                    lastEarmarkAmount = BigDecimal.ZERO;
                    earmarkAmount = BigDecimal.ZERO;
                    lastClientAmount = BigDecimal.ZERO;
                    lastWirecardAmount = BigDecimal.ZERO;
                    clientAccountCurrency = linkedAccount.getInternalAccountCurrency();
                    clientAccountId = linkedAccount.getInternalAccountId();
                } else {
                    throw new Exception("Either last transaction or linked account must be provided");
                }

                messageProcessed = MessageProcessedFactory.from(presentment);

                messageProcessed.setEarmarkAmount(DecimalTypeUtils.toDecimal(earmarkAmount));
                messageProcessed.setEarmarkCurrency(clientAccountCurrency);
                messageProcessed.setTotalEarmarkAmount(DecimalTypeUtils.toDecimal(lastEarmarkAmount.add(earmarkAmount)));
                messageProcessed.setTotalEarmarkCurrency(clientAccountCurrency);

                messageProcessed.setClientAmount(DecimalTypeUtils.toDecimal(clientAmount));
                messageProcessed.setClientCurrency(clientAccountCurrency);
                messageProcessed.setTotalClientAmount(DecimalTypeUtils.toDecimal(lastClientAmount.add(clientAmount)));
                messageProcessed.setTotalClientCurrency(clientAccountCurrency);

                messageProcessed.setWirecardAmount(DecimalTypeUtils.toDecimal(wirecardAmount));
                messageProcessed.setWirecardCurrency(wirecardAccountCurrency);
                messageProcessed.setTotalWirecardAmount(DecimalTypeUtils.toDecimal(lastWirecardAmount.add(wirecardAmount)));
                messageProcessed.setTotalWirecardCurrency(wirecardAccountCurrency);

                messageProcessed.setInternalAccountCurrency(clientAccountCurrency);
                messageProcessed.setInternalAccountId(clientAccountId);
            }

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, originalProcessId);
            values.put(Fields.EVENT_NAME, CardTransactionEvents.RESPONSE_MESSAGE.getEventName());
            values.put(Fields.RESULT, messageProcessed);

            send(tuple, values);
            LOG.info(" Key: {} | ProcessId: {} | Presentment Message Processed. Response sent to kafka topic. GpsTransactionId: {}, Gps TransactionLink: {}",
                    key, originalProcessId, eventData.getProviderMessageId(), eventData.getProviderTransactionId());

        }catch(Exception e){
            LOG.error("Error when generating response message. Tuple: {}, Message: {}, Error: {}", tuple, e.getMessage(), e);
            error(e, tuple);
        }
    }
}
