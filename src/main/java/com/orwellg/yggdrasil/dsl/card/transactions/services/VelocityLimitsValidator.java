package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendGroup;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.yggdrasil.dsl.card.transactions.model.AuthorisationMessage;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

public class VelocityLimitsValidator {
    public ValidationResult validate(AuthorisationMessage message, CardSettings settings, SpendingTotalAmounts totalAmounts) {

        SpendGroup totalType = message.getSpendGroup();
        BigDecimal dailyLimit = getLimit(settings, totalType, "Daily");
        BigDecimal annualLimit = getLimit(settings, totalType, "Annual");
        Boolean isDailyValid = totalAmounts == null
                ||
                getDatePart(totalAmounts.getTimestamp()).before(getDatePart(new Date()))
                ||
                totalAmounts.getDailyTotal().add(message.getSettlementAmount().abs()).compareTo(dailyLimit) <= 0;
        Boolean isAnnualValid = totalAmounts == null
                ||
                getYearPart(totalAmounts.getTimestamp()) < getYearPart(new Date())
                ||
                totalAmounts.getAnnualTotal().add(message.getSettlementAmount().abs()).compareTo(annualLimit) <= 0;
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

    private Date getDatePart(Date dateTime) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(dateTime);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    private Integer getYearPart(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        return cal.get(Calendar.YEAR);
    }

    private BigDecimal getLimit(CardSettings settings, SpendGroup totalType, String dailyAnnual) {
        String key = String.format("%s%s", totalType, dailyAnnual);
        Map<String, BigDecimal> limits = settings.getLimits();
        return limits.get(key);
    }
}
