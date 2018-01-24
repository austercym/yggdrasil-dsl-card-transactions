package com.orwellg.yggdrasil.dsl.card.transactions.utils;

import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;

import java.util.Date;

public final class GpsMessageProcessedFactory {
    public static GpsMessageProcessed from(TransactionInfo transaction){

        GpsMessageProcessed result = new GpsMessageProcessed();
        if (transaction != null) {
            result.setGpsMessageType(transaction.getMessage().getTxnType());
            result.setGpsTransactionLink(transaction.getGpsTransactionLink());
            result.setGpsTransactionId(transaction.getGpsTransactionId());
            result.setDebitCardId(transaction.getDebitCardId());
            result.setSpendGroup(transaction.getSpendGroup());
            result.setTransactionTimestamp(new Date().getTime());
        }
        return result;
    }
}
