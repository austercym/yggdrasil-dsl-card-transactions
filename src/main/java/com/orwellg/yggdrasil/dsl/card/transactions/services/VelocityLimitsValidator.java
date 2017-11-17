package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.cards.SpendGroup;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.yggdrasil.dsl.card.transactions.model.AuthorisationMessage;

import java.math.BigDecimal;
import java.util.Date;
import java.util.Map;

public class VelocityLimitsValidator {

    private static final DateTimeService dateService = new DateTimeService();

    public ValidationResult validate(AuthorisationMessage message, CardSettings settings, SpendingTotalAmounts totalAmounts) {

        SpendGroup totalType = message.getSpendGroup();
        BigDecimal dailyLimit = getLimit(settings, totalType, "Daily");
        BigDecimal annualLimit = getLimit(settings, totalType, "Annual");
        Boolean isDailyValid = totalAmounts == null
                ||
                dateService.getDatePart(totalAmounts.getTimestamp()).before(dateService.getDatePart(new Date()))
                ||
                totalAmounts.getDailyTotal().add(message.getSettlementAmount()).compareTo(dailyLimit) <= 0;
        Boolean isAnnualValid = totalAmounts == null
                ||
                dateService.getYearPart(totalAmounts.getTimestamp()) < dateService.getYearPart(new Date())
                ||
                totalAmounts.getAnnualTotal().add(message.getSettlementAmount()).compareTo(annualLimit) <= 0;
        if (!isDailyValid) {
            return ValidationResult.error(String.format(
                    "Daily limit exceeded (SpendGroup=%f, Limit=%f)", totalAmounts.getDailyTotal(), dailyLimit));
        }
        if (!isAnnualValid) {
            return ValidationResult.error(String.format(
                    "Annual limit exceeded (SpendGroup=%f, Limit=%f)", totalAmounts.getAnnualTotal(), annualLimit));
        }
        return ValidationResult.valid();
    }

    private BigDecimal getLimit(CardSettings settings, SpendGroup totalType, String dailyAnnual) {
        String key = String.format("%s%s", totalType, dailyAnnual);
        Map<String, BigDecimal> limits = settings.getLimits();
        return limits.get(key);
    }
}
