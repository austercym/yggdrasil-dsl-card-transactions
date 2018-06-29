package com.orwellg.yggdrasil.dsl.card.transactions.savetoscylla.bolts;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionMatching;
import com.orwellg.yggdrasil.dsl.card.transactions.common.bolts.GenericEventProcessBolt;
import com.orwellg.yggdrasil.dsl.card.transactions.services.MapperFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.modelmapper.ModelMapper;

import java.time.Instant;
import java.util.Arrays;
import java.util.Map;

public class PrepareDataBolt extends GenericEventProcessBolt<MessageProcessed> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(PrepareDataBolt.class);
    private ModelMapper mapper;

    public PrepareDataBolt() {
        super(MessageProcessed.class);
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        mapper = MapperFactory.getMapper();
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.CARD_TRANSACTION, Fields.TRANSACTION_MATCHING));
    }

    @Override
    protected void process(Map<String, Object> values, MessageProcessed messageProcessed, String key, String processId) {
        CardTransaction cardTransaction = mapEventToCardTransaction(messageProcessed, key, processId);
        values.put(Fields.CARD_TRANSACTION, cardTransaction);

        TransactionMatching transactionMatching = mapper.map(messageProcessed, TransactionMatching.class);
        values.put(Fields.TRANSACTION_MATCHING, transactionMatching);
    }

    private CardTransaction mapEventToCardTransaction(MessageProcessed message, String key, String processId) {

        CardTransaction transaction = new CardTransaction();
        transaction.setProviderTransactionId(message.getProviderTransactionId());
        transaction.setProviderMessageId(message.getProviderMessageId());
        transaction.setProviderTransactionDateTime(Instant.ofEpochMilli(message.getProviderTransactionTime()));
        transaction.setTransactionTimestamp(Instant.ofEpochMilli(message.getTransactionTimestamp()));
        transaction.setMessageType(message.getMessageType());
        transaction.setDebitCardId(message.getDebitCardId());
        transaction.setInternalAccountId(message.getInternalAccountId());
        transaction.setInternalAccountCurrency(message.getInternalAccountCurrency());
        if (message.getTotalWirecardAmount() != null) {
            transaction.setWirecardAmount(message.getTotalWirecardAmount().getValue());
        }
        transaction.setWirecardCurrency(message.getTotalWirecardCurrency());
        if (message.getTotalClientAmount() != null) {
            transaction.setClientAmount(message.getTotalClientAmount().getValue());
        }
        transaction.setClientCurrency(message.getTotalClientCurrency());
        if (message.getTotalEarmarkAmount() != null) {
            transaction.setEarmarkAmount(message.getTotalEarmarkAmount().getValue());
        }
        transaction.setEarmarkCurrency(message.getTotalEarmarkCurrency());
        transaction.setTimestamp(Instant.now());

        LOG.info("[Key: {}][ProcessId: {}]: Card transaction object prepared to save in Scylla.", key, processId);

        return transaction;
    }
}
