package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionType;

import java.util.Set;

public class TransactionTypeValidator implements AuthorisationValidator {

    private TransactionTypeResolver transactionTypeResolver;

    public TransactionTypeValidator(){
        this(new TransactionTypeResolver());
    }

    public TransactionTypeValidator(TransactionTypeResolver transactionTypeResolver) {
        this.transactionTypeResolver = transactionTypeResolver;
    }

    @Override
    public ValidationResult validate(Message message, CardSettings cardSettings) {
        TransactionType transactionType = transactionTypeResolver.getType(message);
        Set<TransactionType> allowedTransactionTypes = cardSettings == null
                ? null
                : cardSettings.getAllowedTransactionTypes();
        return allowedTransactionTypes == null || !allowedTransactionTypes.contains(transactionType)
                ? ValidationResult.error(String.format("%s transaction type is not allowed", transactionType))
                : ValidationResult.valid();
    }
}
