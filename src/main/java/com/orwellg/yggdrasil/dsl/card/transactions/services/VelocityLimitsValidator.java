package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.datastax.driver.core.LocalDate;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TotalSpend;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

public class VelocityLimitsValidator {
    public ValidationResult validate(Message message, CardSettings settings, SpendingTotalAmounts totalAmounts) {

        TotalSpend totalType = getTotalType(message);
        BigDecimal dailyLimit = getLimit(settings, totalType, "Daily");
        BigDecimal annualLimit = getLimit(settings, totalType, "Annual");
        Boolean isDailyValid = totalAmounts == null
                ||
                toDate(totalAmounts.getTimestamp()).before(getDatePart(new Date()))
                ||
                totalAmounts.getDailyTotal().add(BigDecimal.valueOf(message.getSettleAmt())).compareTo(dailyLimit) <= 0;
        Boolean isAnnualValid = totalAmounts == null
                ||
                getYearPart(totalAmounts.getTimestamp()) < getYearPart(new Date())
                ||
                totalAmounts.getAnnualTotal().add(BigDecimal.valueOf(message.getSettleAmt())).compareTo(annualLimit) <= 0;
        if (!isDailyValid) {
            return ValidationResult.error(String.format(
                    "Daily limit exceeded (TotalSpend=%f, Limit=%f)", totalAmounts.getDailyTotal(), dailyLimit));
        }
        if (!isAnnualValid) {
            return ValidationResult.error(String.format(
                    "Annual limit exceeded (TotalSpend=%f, Limit=%f)", totalAmounts.getAnnualTotal(), annualLimit));
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

    private Date toDate(LocalDate localDate) {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.YEAR, localDate.getYear());
        cal.set(Calendar.MONTH, localDate.getMonth());
        cal.set(Calendar.DAY_OF_MONTH, localDate.getDay());
        return cal.getTime();
    }

    private Integer getYearPart(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        return cal.get(Calendar.YEAR);
    }

    private Integer getYearPart(LocalDate date) {
        return date.getYear();
    }

    private BigDecimal getLimit(CardSettings settings, TotalSpend totalType, String dailyAnnual) {
        String key = String.format("%s%s", totalType, dailyAnnual);
        Map<String, BigDecimal> limits = settings.getLimits();
        return limits.get(key);
    }

    private TotalSpend getTotalType(Message message) {
        return Mcc.ATM.equals(message.getMCCCode())
                ? TotalSpend.ATM
                : TotalSpend.POS;
    }
}
