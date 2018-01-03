package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardStatus;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;

public class StatusValidator implements AuthorisationValidator {
    @Override
    public ValidationResult validate(TransactionInfo message, CardSettings cardSettings) {
        return cardSettings == null
                ? ValidationResult.error("Card settings not present")
                : cardSettings.getStatus() == CardStatus.ACTIVE
                ? ValidationResult.valid()
                : ValidationResult.error(String.format("%s card", cardSettings.getStatus()));
    }
}
