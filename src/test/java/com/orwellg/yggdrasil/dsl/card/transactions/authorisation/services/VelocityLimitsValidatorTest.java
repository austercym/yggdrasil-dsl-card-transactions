package com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services;

import com.orwellg.umbrella.avro.types.cards.SpendGroup;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services.ValidationResult;
import com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services.VelocityLimitsValidator;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;

import static org.junit.Assert.*;

public class VelocityLimitsValidatorTest {

    private VelocityLimitsValidator validator = new VelocityLimitsValidator();

    @Test
    public void validateWhenFirstTransactionEverReturnsValid() {
        // arrange
        TransactionInfo message = new TransactionInfo();
        message.setSettlementAmount(BigDecimal.valueOf(19.09));
        message.setSettlementCurrency("EUR");
        message.setSpendGroup(SpendGroup.POS);

        CardSettings settings = new CardSettings();
        settings.setLimits(createLimits(50.0, 200.0));

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
        TransactionInfo message = new TransactionInfo();
        message.setSettlementAmount(BigDecimal.valueOf(19.09));
        message.setSettlementCurrency("EUR");
        message.setSpendGroup(SpendGroup.POS);

        CardSettings settings = new CardSettings();
        settings.setLimits(createLimits(50.0, 200.0));

        SpendingTotalAmounts totalAmounts = getTotal(10, 100, Instant.now());

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
        TransactionInfo message = new TransactionInfo();
        message.setSettlementAmount(BigDecimal.valueOf(19.09));
        message.setSettlementCurrency("EUR");
        message.setSpendGroup(SpendGroup.POS);

        CardSettings settings = new CardSettings();
        settings.setLimits(createLimits(50.0, 200.0));

        SpendingTotalAmounts totalAmounts = getTotal(50, 100, Instant.now().minus(1, ChronoUnit.DAYS));

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
        TransactionInfo message = new TransactionInfo();
        message.setSettlementAmount(BigDecimal.valueOf(19.09));
        message.setSettlementCurrency("EUR");
        message.setSpendGroup(SpendGroup.POS);

        CardSettings settings = new CardSettings();
        settings.setLimits(createLimits(50.0, 200.0));

        SpendingTotalAmounts totalAmounts = getTotal(10, 200, Instant.now().minus(365, ChronoUnit.DAYS));

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
        TransactionInfo message = new TransactionInfo();
        message.setSettlementAmount(BigDecimal.valueOf(19.09));
        message.setSettlementCurrency("EUR");
        message.setSpendGroup(SpendGroup.POS);

        CardSettings settings = new CardSettings();
        settings.setLimits(createLimits(30.0, 200.0));

        SpendingTotalAmounts totalAmounts = getTotal(20, 20, Instant.now());

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
        TransactionInfo message = new TransactionInfo();
        message.setSettlementAmount(BigDecimal.valueOf(19.09));
        message.setSettlementCurrency("EUR");
        message.setSpendGroup(SpendGroup.POS);

        CardSettings settings = new CardSettings();
        settings.setLimits(createLimits(50.0, 200.0));

        SpendingTotalAmounts totalAmounts = getTotal(0, 190, Instant.now());

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

    private SpendingTotalAmounts getTotal(double daily, double annual, Instant timestamp) {
        SpendingTotalAmounts total = new SpendingTotalAmounts();
        total.setDailyTotal(BigDecimal.valueOf(daily));
        total.setAnnualTotal(BigDecimal.valueOf(annual));
        total.setTimestamp(timestamp);
        return total;
    }
}
