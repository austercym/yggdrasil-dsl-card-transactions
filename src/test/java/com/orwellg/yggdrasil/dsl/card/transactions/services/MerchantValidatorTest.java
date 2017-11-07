package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.yggdrasil.dsl.card.transactions.model.AuthorisationMessage;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import static org.junit.Assert.*;

public class MerchantValidatorTest {

    private final MerchantValidator validator = new MerchantValidator();

    @Test
    public void validateWhenCardNotPresentAndMerchantNotOnTheListReturnsNotValid() {
        // arrange
        AuthorisationMessage message = new AuthorisationMessage();
        message.setMerchantId("bar");
        message.setIsCardPresent(false);

        CardSettings cardSettings = new CardSettings();

        HashMap<String, Date> allowedMerchants = new HashMap<>();
        allowedMerchants.put("foo", new Date());
        cardSettings.setAllowedCardNotPresentMerchants(allowedMerchants);

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
        AuthorisationMessage message = new AuthorisationMessage();
        message.setMerchantId("foo");
        message.setIsCardPresent(false);

        CardSettings cardSettings = new CardSettings();

        HashMap<String, Date> allowedMerchants = new HashMap<>();
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.YEAR, -1);
        allowedMerchants.put("foo", cal.getTime());
        cardSettings.setAllowedCardNotPresentMerchants(allowedMerchants);

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
        AuthorisationMessage message = new AuthorisationMessage();
        message.setMerchantId("foo");
        message.setIsCardPresent(false);

        CardSettings cardSettings = new CardSettings();

        HashMap<String, Date> allowedMerchants = new HashMap<>();
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.YEAR, 1);
        allowedMerchants.put("foo", cal.getTime());
        cardSettings.setAllowedCardNotPresentMerchants(allowedMerchants);

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
        AuthorisationMessage message = new AuthorisationMessage();
        message.setMerchantId("foo");
        message.setIsCardPresent(false);

        CardSettings cardSettings = new CardSettings();

        HashMap<String, Date> allowedMerchants = new HashMap<>();
        allowedMerchants.put("foo", null);
        cardSettings.setAllowedCardNotPresentMerchants(allowedMerchants);

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
        AuthorisationMessage message = new AuthorisationMessage();
        message.setMerchantId("foo");
        message.setIsCardPresent(true);

        CardSettings cardSettings = new CardSettings();

        // act
        ValidationResult result = validator.validate(message, cardSettings);

        // assert
        assertNotNull(result);
        assertTrue(result.getIsValid());
        assertNull(result.getMessage());
    }
}
