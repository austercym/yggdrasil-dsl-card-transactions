package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;

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

    public SpendingTotalAmounts recalculate(GpsMessageProcessed messageProcessed, SpendingTotalAmounts lastSpendingTotalAmounts) {

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
            daily = daily.add(messageProcessed.getBlockedClientAmount().getValue().abs());

        if (transactionTimestamp.getYear() == today.getYear() && messageProcessed.getBlockedClientAmount() != null)
            annual = annual.add(messageProcessed.getBlockedClientAmount().getValue().abs());

        SpendingTotalAmounts result = new SpendingTotalAmounts();
        result.setAnnualTotal(annual);
        result.setDailyTotal(daily);
        result.setTimestamp(now);
        result.setDebitCardId(messageProcessed.getDebitCardId());
        result.setTotalType(messageProcessed.getSpendGroup());
        return result;
    }

    public boolean isRequired(GpsMessageProcessed message) {
        return !"A".equals(message.getGpsMessageType()) || "00".equals(message.getEhiResponse().getResponsestatus());
    }
}
