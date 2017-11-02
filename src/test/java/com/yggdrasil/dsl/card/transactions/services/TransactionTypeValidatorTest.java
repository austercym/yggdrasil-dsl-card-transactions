package com.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;


import java.util.Arrays;
import java.util.HashSet;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TransactionTypeValidatorTest {

    @Mock
    private TransactionTypeResolver transactionTypeResolverMock;

    private TransactionTypeValidator validator;

    @Before
    public void initialize() {
        validator = new TransactionTypeValidator(transactionTypeResolverMock);
    }

    @Test
    public void validateWhenTransactionTypeAllowedReturnsIsValid() {
        // arrange
        Message message = new Message();
        CardSettings cardSettings = new CardSettings();
        cardSettings.setAllowedTransactionTypes(new HashSet<>(Arrays.asList(TransactionType.ONLINE)));
        when(transactionTypeResolverMock.getType(any(Message.class))).thenReturn(TransactionType.ONLINE);

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
        Message message = new Message();
        CardSettings cardSettings = new CardSettings();
        cardSettings.setAllowedTransactionTypes(new HashSet<>(Arrays.asList(TransactionType.ONLINE)));
        when(transactionTypeResolverMock.getType(any(Message.class))).thenReturn(TransactionType.POS);

        // act
        ValidationResult result = validator.validate(message, cardSettings);

        // assert
        assertNotNull(result);
        assertFalse(result.getIsValid());
        assertNotNull(result.getMessage());
    }
}
