package com.orwellg.yggdrasil.dsl.card.transactions.utils;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;

import java.time.ZoneOffset;
import java.util.Objects;

public final class MessageProcessedFactory {
    public static MessageProcessed from(TransactionInfo transaction) {

        MessageProcessed result = new MessageProcessed();
        if (transaction != null) {
            Objects.requireNonNull(transaction.getMessage(), "TransactionInfo.Message cannot be null");

            result.setMessageType(MessageTypeMapper.fromGpsTxnType(transaction.getMessage().getTxnType()));
            result.setProviderTransactionId(transaction.getGpsTransactionLink());
            result.setProviderMessageId(transaction.getGpsTransactionId());
            result.setDebitCardId(transaction.getDebitCardId());
            result.setSpendGroup(transaction.getSpendGroup());
            if (transaction.getTransactionDateTime() != null) {
                result.setTransactionTimestamp(
                        transaction.getTransactionDateTime().toInstant(ZoneOffset.UTC).toEpochMilli());
            }
            if (transaction.getGpsTransactionTime() != null) {
                result.setProviderTransactionTime(
                        transaction.getGpsTransactionTime().toInstant(ZoneOffset.UTC).toEpochMilli());
            }
        }
        return result;
    }
}
