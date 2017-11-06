package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.datastax.driver.core.LocalDate;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;

import static org.junit.Assert.*;

public class VelocityLimitsValidatorTest {

    private VelocityLimitsValidator validator = new VelocityLimitsValidator();

    @Test
    public void validateWhenFirstTransactionEverReturnsValid() {
        // arrange
        Message message = new Message();
        message.setSettleAmt(19.09);
        message.setSettleCcy("978");
        CardSettings settings = new CardSettings();
        HashMap<String, BigDecimal> limits = new HashMap<>();
        limits.put("EUR", BigDecimal.valueOf(200.0));
        settings.setLimits(limits);

        // act
        ValidationResult result = validator.validate(message, settings, null);

        // assert
        assertNotNull(result);
        assertTrue(result.getIsValid());
        assertNull(result.getMessage());
    }

    @Test
    public void validateWhenLimitsNotExceededReturnsValid() {
        // arrange
        Message message = new Message();
        message.setSettleAmt(19.09);
        message.setSettleCcy("978");

        CardSettings settings = new CardSettings();
        settings.setLimits(createLimits(50.0, 200.0));

        SpendingTotalAmounts totalAmounts = getTotal(10, 100, new Date());

        // act
        ValidationResult result = validator.validate(message, settings, totalAmounts);

        // assert
        assertNotNull(result);
        assertTrue(result.getIsValid());
        assertNull(result.getMessage());
    }

    @Test
    public void validateWhenDailyLimitsExpiredReturnsValid() {
        // arrange
        Message message = new Message();
        message.setSettleAmt(19.09);
        message.setSettleCcy("978");

        CardSettings settings = new CardSettings();
        settings.setLimits(createLimits(50.0, 200.0));

        SpendingTotalAmounts totalAmounts = getTotal(50, 100, yesterday());

        // act
        ValidationResult result = validator.validate(message, settings, totalAmounts);

        // assert
        assertNotNull(result);
        assertTrue(result.getIsValid());
        assertNull(result.getMessage());
    }

    @Test
    public void validateWhenAnnualLimitsExpiredReturnsValid() {
        // arrange
        Message message = new Message();
        message.setSettleAmt(19.09);
        message.setSettleCcy("978");

        CardSettings settings = new CardSettings();
        settings.setLimits(createLimits(50.0, 200.0));

        SpendingTotalAmounts totalAmounts = getTotal(10, 200, lastYear());

        // act
        ValidationResult result = validator.validate(message, settings, totalAmounts);

        // assert
        assertNotNull(result);
        assertTrue(result.getIsValid());
        assertNull(result.getMessage());
    }

    @Test
    public void validateWhenDailyLimitExceededReturnsInvalid() {
        // arrange
        Message message = new Message();
        message.setSettleAmt(19.09);
        message.setSettleCcy("978");

        CardSettings settings = new CardSettings();
        settings.setLimits(createLimits(30.0, 200.0));

        SpendingTotalAmounts totalAmounts = getTotal(20, 20, new Date());

        // act
        ValidationResult result = validator.validate(message, settings, totalAmounts);

        // assert
        assertNotNull(result);
        assertFalse(result.getIsValid());
        assertNotNull(result.getMessage());
    }

    @Test
    public void validateWhenAnnualLimitExceededReturnsInvalid() {
        // arrange
        Message message = new Message();
        message.setSettleAmt(19.09);
        message.setSettleCcy("978");

        CardSettings settings = new CardSettings();
        settings.setLimits(createLimits(50.0, 200.0));

        SpendingTotalAmounts totalAmounts = getTotal(0, 190, new Date());

        // act
        ValidationResult result = validator.validate(message, settings, totalAmounts);

        // assert
        assertNotNull(result);
        assertFalse(result.getIsValid());
        assertNotNull(result.getMessage());
    }

    private HashMap<String, BigDecimal> createLimits(double posDailyLimit, double posAnnualLimit) {
        HashMap<String, BigDecimal> limits = new HashMap<>();
        limits.put("POSDaily", BigDecimal.valueOf(posDailyLimit));
        limits.put("POSAnnual", BigDecimal.valueOf(posAnnualLimit));
        return limits;
    }

    private SpendingTotalAmounts getTotal(double daily, double annual, Date timestamp) {
        SpendingTotalAmounts total = new SpendingTotalAmounts();
        total.setDailyTotal(BigDecimal.valueOf(daily));
        total.setAnnualTotal(BigDecimal.valueOf(annual));
        total.setTimestamp(timestamp);
        return total;
    }

    private Date yesterday() {
        final Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -1);
        return cal.getTime();
    }

    private Date lastYear() {
        final Calendar cal = Calendar.getInstance();
        cal.add(Calendar.YEAR, -1);
        return cal.getTime();
    }
}
