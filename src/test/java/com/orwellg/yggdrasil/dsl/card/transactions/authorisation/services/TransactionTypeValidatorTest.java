package com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionType;
import com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services.TransactionTypeValidator;
import com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services.ValidationResult;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import junit.framework.TestCase;
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
        TestCase.assertNotNull(result);
        TestCase.assertTrue(result.getIsValid());
        TestCase.assertNull(result.getMessage());
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
        TestCase.assertNotNull(result);
        TestCase.assertFalse(result.getIsValid());
        TestCase.assertNotNull(result.getMessage());
    }
}
