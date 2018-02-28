package com.orwellg.yggdrasil.dsl.card.transactions.utils;

import com.orwellg.umbrella.avro.types.cards.CardMessageProcessed;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;

import java.time.ZoneOffset;
import java.util.Objects;

public final class CardMessageProcessedFactory {
    public static CardMessageProcessed from(TransactionInfo transaction) {

        CardMessageProcessed result = new CardMessageProcessed();
        if (transaction != null) {
            Objects.requireNonNull(transaction.getMessage(), "TransactionInfo.Message cannot be null");

            result.setGpsMessageType(transaction.getMessage().getTxnType());
            result.setGpsTransactionLink(transaction.getGpsTransactionLink());
            result.setGpsTransactionId(transaction.getGpsTransactionId());
            result.setDebitCardId(transaction.getDebitCardId());
            result.setSpendGroup(transaction.getSpendGroup());
            if (transaction.getTransactionDateTime() != null) {
                result.setTransactionTimestamp(
                        transaction.getTransactionDateTime().toInstant(ZoneOffset.UTC).toEpochMilli());
            }
            if (transaction.getGpsTransactionTime() != null) {
                result.setGpsTransactionTime(
                        transaction.getGpsTransactionTime().toInstant(ZoneOffset.UTC).toEpochMilli());
            }
        }
        return result;
    }
}
