package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.cards.SpendGroup;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Map;

public class VelocityLimitsValidator {

    public ValidationResult validate(TransactionInfo message, CardSettings settings, SpendingTotalAmounts totalAmounts) {

        if (settings == null) {
            return ValidationResult.error("Card settings not present");
        }
        SpendGroup totalType = message.getSpendGroup();
        BigDecimal dailyLimit = getLimit(settings, totalType, "Daily");
        BigDecimal annualLimit = getLimit(settings, totalType, "Annual");
        LocalDate lastUpdateDate = totalAmounts == null
                ? null
                : totalAmounts.getTimestamp().atZone(ZoneId.of("UTC")).toLocalDate();
        Boolean isDailyValid = lastUpdateDate == null
                ||
                lastUpdateDate.isBefore(LocalDate.now())
                ||
                totalAmounts.getDailyTotal().add(message.getSettlementAmount()).compareTo(dailyLimit) <= 0;
        Boolean isAnnualValid = lastUpdateDate == null
                ||
                lastUpdateDate.getYear() < LocalDate.now().getYear()
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
