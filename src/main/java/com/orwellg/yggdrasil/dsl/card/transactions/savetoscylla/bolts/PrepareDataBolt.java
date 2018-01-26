package com.orwellg.yggdrasil.dsl.card.transactions.savetoscylla.bolts;

import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.dsl.card.transactions.common.bolts.GenericEventProcessBolt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;

public class PrepareDataBolt extends GenericEventProcessBolt<GpsMessageProcessed> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(PrepareDataBolt.class);

    public PrepareDataBolt() {
        super(GpsMessageProcessed.class);
    }

    @Override
    protected Object process(GpsMessageProcessed message, String key, String processId) {

        CardTransaction transaction = new CardTransaction();
        transaction.setGpsTransactionLink(message.getGpsTransactionLink());
        transaction.setGpsTransactionId(message.getGpsTransactionId());
        transaction.setGpsTransactionDateTime(Instant.ofEpochMilli(message.getGpsTransactionTime()));
        transaction.setTransactionTimestamp(Instant.ofEpochMilli(message.getTransactionTimestamp()));
        transaction.setGpsMessageType(message.getGpsMessageType());
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
        if (message.getTotalFeesAmount() != null) {
            transaction.setFeesAmount(message.getTotalFeesAmount().getValue());
        }
        transaction.setFeesCurrency(message.getTotalFeesCurrency());

        LOG.info("[Key: {}][ProcessId: {}]: Card transaction object prepared to save in Scylla.", key, processId);

        return transaction;
    }
}
