package com.orwellg.yggdrasil.dsl.card.transactions.presentment.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.dsl.card.transactions.model.GpsMessageProcessingException;
import com.orwellg.yggdrasil.dsl.card.transactions.model.PresentmentErrorCode;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class AuthorisationValidationService {

    private List<String>  validGpsMessageTypes =  Arrays.asList("A", "D", "P");

    public CardTransaction getLast(List<CardTransaction> cardTransactions, String transactionId) throws Exception{

        CardTransaction lastTransaction = null;

        Optional<CardTransaction> max = cardTransactions.stream()
                .max(Comparator.comparing(CardTransaction::getTransactionTimestamp));

        if (max.isPresent()) {
            lastTransaction = max.get();
            String lastTransactionType = lastTransaction.getGpsMessageType();

            if (!validGpsMessageTypes.contains(lastTransactionType))
                throw new GpsMessageProcessingException(PresentmentErrorCode.INVALID_PREVIOUS_TRANSACTION_TYPE, "Error when processing presentment - invalid last transaction type: " + lastTransactionType+ ". Valid types are: A, D, P");

            //if last transaction id is the same as current - this is an error
            if (lastTransaction.getGpsTransactionId().equals(transactionId))
                throw new GpsMessageProcessingException(PresentmentErrorCode.DUPLICATED_TRANSACTION_ID, "Error when processing presentment - last processed transaction has the same transactionId: " + lastTransaction.getGpsTransactionId());
        }

        return lastTransaction;
    }

}
