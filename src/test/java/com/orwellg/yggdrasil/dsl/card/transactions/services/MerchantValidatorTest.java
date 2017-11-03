package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MerchantValidatorTest {

    @Mock
    private CardPresenceResolver cardPresenceResolverMock;

    private MerchantValidator validator;

    @Before
    public void initialize() {
        validator = new MerchantValidator(cardPresenceResolverMock);
    }

    @Test
    public void validateWhenCardNotPresentAndMerchantNotOnTheListReturnsNotValid() {
        // arrange
        Message message = new Message();
        message.setMerchIDDE42("bar");

        CardSettings cardSettings = new CardSettings();

        HashMap<String, Date> allowedMerchants = new HashMap<>();
        allowedMerchants.put("foo", new Date());
        cardSettings.setAllowedCardNotPresentMerchants(allowedMerchants);

        when(cardPresenceResolverMock.isCardPresent(any())).thenReturn(false);

        // act
        ValidationResult result = validator.validate(message, cardSettings);

        // assert
        assertNotNull(result);
        assertFalse(result.getIsValid());
        assertNotNull(result.getMessage());
    }

    @Test
    public void validateWhenCardNotPresentAndMerchantIsExpiredReturnsNotValid() {
        // arrange
        Message message = new Message();
        message.setMerchIDDE42("foo");

        CardSettings cardSettings = new CardSettings();

        HashMap<String, Date> allowedMerchants = new HashMap<>();
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.YEAR, -1);
        allowedMerchants.put("foo", cal.getTime());
        cardSettings.setAllowedCardNotPresentMerchants(allowedMerchants);

        when(cardPresenceResolverMock.isCardPresent(any())).thenReturn(false);

        // act
        ValidationResult result = validator.validate(message, cardSettings);

        // assert
        assertNotNull(result);
        assertFalse(result.getIsValid());
        assertNotNull(result.getMessage());
    }

    @Test
    public void validateWhenCardNotPresentAndMerchantIsNotExpiredReturnsIsValid() {
        // arrange
        Message message = new Message();
        message.setMerchIDDE42("foo");

        CardSettings cardSettings = new CardSettings();

        HashMap<String, Date> allowedMerchants = new HashMap<>();
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.YEAR, 1);
        allowedMerchants.put("foo", cal.getTime());
        cardSettings.setAllowedCardNotPresentMerchants(allowedMerchants);

        when(cardPresenceResolverMock.isCardPresent(any())).thenReturn(false);

        // act
        ValidationResult result = validator.validate(message, cardSettings);

        // assert
        assertNotNull(result);
        assertTrue(result.getIsValid());
        assertNull(result.getMessage());
    }

    @Test
    public void validateWhenCardNotPresentAndDatelessMerchantReturnsIsValid() {
        // arrange
        Message message = new Message();
        message.setMerchIDDE42("foo");

        CardSettings cardSettings = new CardSettings();

        HashMap<String, Date> allowedMerchants = new HashMap<>();
        allowedMerchants.put("foo", null);
        cardSettings.setAllowedCardNotPresentMerchants(allowedMerchants);

        when(cardPresenceResolverMock.isCardPresent(any())).thenReturn(false);

        // act
        ValidationResult result = validator.validate(message, cardSettings);

        // assert
        assertNotNull(result);
        assertTrue(result.getIsValid());
        assertNull(result.getMessage());
    }

    @Test
    public void validateWhenCardPresentReturnsIsValid() {
        // arrange
        Message message = new Message();
        message.setMerchIDDE42("foo");

        CardSettings cardSettings = new CardSettings();

        when(cardPresenceResolverMock.isCardPresent(any())).thenReturn(true);

        // act
        ValidationResult result = validator.validate(message, cardSettings);

        // assert
        assertNotNull(result);
        assertTrue(result.getIsValid());
        assertNull(result.getMessage());
    }
}
