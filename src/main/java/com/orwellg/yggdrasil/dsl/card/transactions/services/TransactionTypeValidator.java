package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionType;
import com.orwellg.yggdrasil.dsl.card.transactions.model.AuthorisationMessage;

import java.util.Set;

public class TransactionTypeValidator implements AuthorisationValidator {
    @Override
    public ValidationResult validate(AuthorisationMessage message, CardSettings cardSettings) {
        TransactionType transactionType = message.getTransactionType();
        Set<TransactionType> allowedTransactionTypes = cardSettings == null
                ? null
                : cardSettings.getAllowedTransactionTypes();
        return allowedTransactionTypes == null || !allowedTransactionTypes.contains(transactionType)
                ? ValidationResult.error(String.format("%s transaction type is not allowed", transactionType))
                : ValidationResult.valid();
    }
}
