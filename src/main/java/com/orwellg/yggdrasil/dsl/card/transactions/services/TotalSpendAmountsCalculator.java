package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;

import java.math.BigDecimal;
import java.util.Date;

public class TotalSpendAmountsCalculator {

    private DateTimeService dateTimeService;

    public TotalSpendAmountsCalculator() {
        this(new DateTimeService());
    }

    TotalSpendAmountsCalculator(DateTimeService dateTimeService) {
        this.dateTimeService = dateTimeService;
    }

    public SpendingTotalAmounts recalculate(GpsMessageProcessed messageProcessed, SpendingTotalAmounts lastSpendingTotalAmounts) {

        Date now = dateTimeService.now();
        BigDecimal daily = lastSpendingTotalAmounts != null
                &&
                isSameDay(lastSpendingTotalAmounts.getTimestamp(), now) ?
                lastSpendingTotalAmounts.getDailyTotal() :
                BigDecimal.ZERO;
        BigDecimal annual = lastSpendingTotalAmounts != null
                &&
                isSameYear(lastSpendingTotalAmounts.getTimestamp(), now) ?
                lastSpendingTotalAmounts.getAnnualTotal() :
                BigDecimal.ZERO;

        if (isSameDay(messageProcessed.getTransactionTimestamp(), now))
            daily = daily.add(messageProcessed.getBlockedClientAmount().getValue().abs());

        if (isSameYear(messageProcessed.getTransactionTimestamp(), now))
            annual = annual.add(messageProcessed.getBlockedClientAmount().getValue().abs());

        SpendingTotalAmounts result = new SpendingTotalAmounts();
        result.setAnnualTotal(annual);
        result.setDailyTotal(daily);
        result.setTimestamp(now);
        result.setDebitCardId(messageProcessed.getDebitCardId());
        result.setTotalType(messageProcessed.getSpendGroup());
        return result;
    }

    private boolean isSameDay(Long timestamp, Date now) {
         return isSameDay(new Date(timestamp), now);
    }

    private boolean isSameDay(Date timestamp, Date now) {
        return timestamp != null && now != null
                &&
                dateTimeService.getDatePart(timestamp).equals(dateTimeService.getDatePart(now));
    }

    private boolean isSameYear(Long timestamp, Date now) {
        return isSameYear(new Date(timestamp), now);
    }

    private boolean isSameYear(Date timestamp, Date now) {
        return timestamp != null && now != null
                &&
                dateTimeService.getYearPart(timestamp).equals(dateTimeService.getYearPart(now));
    }

    public boolean isRequired(GpsMessageProcessed message) {
        return !"A".equals(message.getGpsMessageType()) || "00".equals(message.getEhiResponse().getResponsestatus());
    }
}
