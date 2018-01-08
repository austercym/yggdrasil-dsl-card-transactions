package com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionType;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;

import java.util.Set;

public class TransactionTypeValidator implements AuthorisationValidator {
    @Override
    public ValidationResult validate(TransactionInfo message, CardSettings cardSettings) {
        TransactionType transactionType = message.getTransactionType();
        Set<TransactionType> allowedTransactionTypes = cardSettings == null
                ? null
                : cardSettings.getAllowedTransactionTypes();

        // Temp currency check
        if (message.getSettlementCurrency() == null
                ||
                cardSettings == null
                ||
                !message.getSettlementCurrency().equals(cardSettings.getLinkedAccountCurrency()))
            return ValidationResult.error("Transaction currency must match linked account currency");

        return allowedTransactionTypes == null || !allowedTransactionTypes.contains(transactionType)
                ? ValidationResult.error(String.format("%s transaction type is not allowed", transactionType))
                : ValidationResult.valid();
    }
}
