package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.yggdrasil.dsl.card.transactions.model.AuthorisationMessage;

public interface AuthorisationValidator {
    ValidationResult validate(AuthorisationMessage message, CardSettings cardSettings);
}
