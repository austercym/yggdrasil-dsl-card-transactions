package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;

public interface AuthorisationValidator {
    ValidationResult validate(TransactionInfo message, CardSettings cardSettings);
}
