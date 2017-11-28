package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.commons.Decimal;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalEarmark;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class TotalSpendAmountsCalculator {

    private DateTimeService dateTimeService;

    public TotalSpendAmountsCalculator() {
        this(new DateTimeService());
    }

    TotalSpendAmountsCalculator(DateTimeService dateTimeService) {
        this.dateTimeService = dateTimeService;
    }

    public SpendingTotalAmounts recalculate(GpsMessageProcessed messageProcessed, SpendingTotalAmounts lastSpendingTotalAmounts, SpendingTotalEarmark earmark) {

        Instant now = dateTimeService.now();
        LocalDate today = now.atZone(ZoneId.of("UTC")).toLocalDate();
        LocalDateTime transactionTimestamp = LocalDateTime.from(
                Instant.ofEpochMilli(messageProcessed.getTransactionTimestamp()).atZone(ZoneId.of("UTC")));
        LocalDate lastUpdateDate = lastSpendingTotalAmounts == null || lastSpendingTotalAmounts.getTimestamp() == null
                ? null
                : lastSpendingTotalAmounts.getTimestamp().atZone(ZoneId.of("UTC")).toLocalDate();
        BigDecimal daily = lastUpdateDate != null
                &&
                lastUpdateDate.equals(today) ?
                lastSpendingTotalAmounts.getDailyTotal() :
                BigDecimal.ZERO;
        BigDecimal annual = lastUpdateDate != null
                &&
                lastUpdateDate.getYear() == today.getYear() ?
                lastSpendingTotalAmounts.getAnnualTotal() :
                BigDecimal.ZERO;

        if (transactionTimestamp.toLocalDate().equals(today) && messageProcessed.getBlockedClientAmount() != null)
            daily = calculateNewAmount(daily, messageProcessed.getBlockedClientAmount(), earmark);

        if (transactionTimestamp.getYear() == today.getYear() && messageProcessed.getBlockedClientAmount() != null)
            annual = calculateNewAmount(annual, messageProcessed.getBlockedClientAmount(), earmark);

        SpendingTotalAmounts result = new SpendingTotalAmounts();
        result.setAnnualTotal(annual);
        result.setDailyTotal(daily);
        result.setTimestamp(now);
        result.setDebitCardId(messageProcessed.getDebitCardId());
        result.setTotalType(messageProcessed.getSpendGroup());
        return result;
    }

    private BigDecimal calculateNewAmount(BigDecimal currentAmount, Decimal blockedClientAmount, SpendingTotalEarmark earmark) {
        BigDecimal newAmount = currentAmount.add(blockedClientAmount.getValue().abs());

        if (earmark != null) {
            newAmount = newAmount.subtract(earmark.getAmount().abs());
        }
        return newAmount;
    }

    public boolean isRequired(GpsMessageProcessed message) {
        return !"A".equals(message.getGpsMessageType()) || "00".equals(message.getEhiResponse().getResponsestatus());
    }
}
