package com.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardStatus;

public class StatusValidationServiceImpl implements AuthorisationValidationService {
    @Override
    public ValidationResult validate(Message message, CardSettings cardSettings) {
        return cardSettings.getStatus() == CardStatus.ACTIVE
                ? ValidationResult.Valid()
                : ValidationResult.Error(String.format("%s card", cardSettings.getStatus()));
    }
}
