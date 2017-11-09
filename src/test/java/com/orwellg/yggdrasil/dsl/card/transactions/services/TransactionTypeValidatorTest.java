package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionType;
import com.orwellg.yggdrasil.dsl.card.transactions.model.AuthorisationMessage;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.assertNull;

public class TransactionTypeValidatorTest {

    private final TransactionTypeValidator validator = new TransactionTypeValidator();

    @Test
    public void validateWhenTransactionTypeAllowedReturnsIsValid() {
        // arrange
        AuthorisationMessage message = new AuthorisationMessage();
        message.setTransactionType(TransactionType.ONLINE);
        CardSettings cardSettings = new CardSettings();
        cardSettings.setAllowedTransactionTypes(new HashSet<>(Arrays.asList(TransactionType.ONLINE)));

        // act
        ValidationResult result = validator.validate(message, cardSettings);

        // assert
        assertNotNull(result);
        assertTrue(result.getIsValid());
        assertNull(result.getMessage());
    }

    @Test
    public void validateWhenTransactionTypeNotAllowedReturnsNotValid() {
        // arrange
        AuthorisationMessage message = new AuthorisationMessage();
        message.setTransactionType(TransactionType.POS);
        CardSettings cardSettings = new CardSettings();
        cardSettings.setAllowedTransactionTypes(new HashSet<>(Arrays.asList(TransactionType.ONLINE)));

        // act
        ValidationResult result = validator.validate(message, cardSettings);

        // assert
        assertNotNull(result);
        assertFalse(result.getIsValid());
        assertNotNull(result.getMessage());
    }
}
