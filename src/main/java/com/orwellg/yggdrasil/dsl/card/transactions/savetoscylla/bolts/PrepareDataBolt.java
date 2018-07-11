package com.orwellg.yggdrasil.dsl.card.transactions.savetoscylla.bolts;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.card.transaction.commons.bolts.GenericEventProcessBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;

public class PrepareDataBolt extends GenericEventProcessBolt<MessageProcessed> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(PrepareDataBolt.class);

    public PrepareDataBolt() {
        super(MessageProcessed.class);
    }

    @Override
    protected Object process(MessageProcessed message, String key, String processId) {

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
