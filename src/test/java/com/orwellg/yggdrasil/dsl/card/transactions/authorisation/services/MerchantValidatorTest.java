package com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services.MerchantValidator;
import com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services.ValidationResult;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import org.junit.Assert;
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
        TransactionInfo message = new TransactionInfo();
        message.setMerchantId("bar");
        message.setIsCardPresent(false);

        CardSettings cardSettings = new CardSettings();

        HashMap<String, Date> allowedMerchants = new HashMap<>();
        allowedMerchants.put("foo", new Date());
        cardSettings.setAllowedCardNotPresentMerchants(allowedMerchants);

        // act
        ValidationResult result = validator.validate(message, cardSettings);

        // assert
        Assert.assertNotNull(result);
        Assert.assertFalse(result.getIsValid());
        Assert.assertNotNull(result.getMessage());
    }

    @Test
    public void validateWhenCardNotPresentAndMerchantIsExpiredReturnsNotValid() {
        // arrange
        TransactionInfo message = new TransactionInfo();
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
        Assert.assertNotNull(result);
        Assert.assertFalse(result.getIsValid());
        Assert.assertNotNull(result.getMessage());
    }

    @Test
    public void validateWhenCardNotPresentAndMerchantIsNotExpiredReturnsIsValid() {
        // arrange
        TransactionInfo message = new TransactionInfo();
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
        Assert.assertNotNull(result);
        Assert.assertTrue(result.getIsValid());
        Assert.assertNull(result.getMessage());
    }

    @Test
    public void validateWhenCardNotPresentAndDatelessMerchantReturnsIsValid() {
        // arrange
        TransactionInfo message = new TransactionInfo();
        message.setMerchantId("foo");
        message.setIsCardPresent(false);

        CardSettings cardSettings = new CardSettings();

        HashMap<String, Date> allowedMerchants = new HashMap<>();
        allowedMerchants.put("foo", null);
        cardSettings.setAllowedCardNotPresentMerchants(allowedMerchants);

        // act
        ValidationResult result = validator.validate(message, cardSettings);

        // assert
        Assert.assertNotNull(result);
        Assert.assertTrue(result.getIsValid());
        Assert.assertNull(result.getMessage());
    }

    @Test
    public void validateWhenCardPresentReturnsIsValid() {
        // arrange
        TransactionInfo message = new TransactionInfo();
        message.setMerchantId("foo");
        message.setIsCardPresent(true);

        CardSettings cardSettings = new CardSettings();

        // act
        ValidationResult result = validator.validate(message, cardSettings);

        // assert
        Assert.assertNotNull(result);
        Assert.assertTrue(result.getIsValid());
        Assert.assertNull(result.getMessage());
    }
}
