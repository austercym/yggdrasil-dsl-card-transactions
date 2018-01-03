package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionType;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import org.junit.Test;

import java.util.Collections;
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
        TransactionInfo message = new TransactionInfo();
        message.setTransactionType(TransactionType.ONLINE);
        message.setSettlementCurrency("ZLT");
        CardSettings cardSettings = new CardSettings();
        cardSettings.setAllowedTransactionTypes(new HashSet<>(Collections.singletonList(TransactionType.ONLINE)));
        cardSettings.setLinkedAccountCurrency("ZLT");

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
        TransactionInfo message = new TransactionInfo();
        message.setTransactionType(TransactionType.POS);
        message.setSettlementCurrency("ZLT");
        CardSettings cardSettings = new CardSettings();
        cardSettings.setAllowedTransactionTypes(new HashSet<>(Collections.singletonList(TransactionType.ONLINE)));
        cardSettings.setLinkedAccountCurrency("ZLT");

        // act
        ValidationResult result = validator.validate(message, cardSettings);

        // assert
        assertNotNull(result);
        assertFalse(result.getIsValid());
        assertNotNull(result.getMessage());
    }
}
