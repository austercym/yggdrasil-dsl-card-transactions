package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.commons.Decimal;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionEarmark;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;

public class TotalSpendAmountsCalculator {

    private static final Logger LOG = LogManager.getLogger(TotalSpendAmountsCalculator.class);

    private DateTimeService dateTimeService;

    public TotalSpendAmountsCalculator() {
        this(new DateTimeService());
    }

    TotalSpendAmountsCalculator(DateTimeService dateTimeService) {
        this.dateTimeService = dateTimeService;
    }

    public SpendingTotalAmounts recalculate(GpsMessageProcessed messageProcessed, SpendingTotalAmounts lastSpendingTotalAmounts, TransactionEarmark earmark) {

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

        if (messageProcessed.getBlockedClientAmount() == null) {
            LOG.info(
                    "Spending total amounts will not be updated - BlockedClientAmount is not set for GpsTransactionLink={}",
                    messageProcessed.getGpsTransactionLink());
        } else {
            if (transactionTimestamp.toLocalDate().equals(today)) {
                daily = calculateNewAmount(daily, messageProcessed.getBlockedClientAmount(), earmark);
            } else {
                LOG.info(
                        "Daily spending total amount will not be updated - GpsTransactionLink={}, TransactionTimestamp={}",
                        messageProcessed.getGpsTransactionLink(), transactionTimestamp);
            }

            if (transactionTimestamp.getYear() == today.getYear()) {
                annual = calculateNewAmount(annual, messageProcessed.getBlockedClientAmount(), earmark);
            } else {
                LOG.info(
                        "Annual spending total amount will not be updated - GpsTransactionLink={}, TransactionTimestamp={}",
                        messageProcessed.getGpsTransactionLink(), transactionTimestamp);
            }
        }

        SpendingTotalAmounts result = new SpendingTotalAmounts();
        result.setAnnualTotal(annual);
        result.setDailyTotal(daily);
        result.setTimestamp(now);
        result.setDebitCardId(messageProcessed.getDebitCardId());
        result.setTotalType(messageProcessed.getSpendGroup());
        return result;
    }

    private BigDecimal calculateNewAmount(BigDecimal currentAmount, Decimal blockedClientAmount, TransactionEarmark earmark) {
        BigDecimal newAmount = currentAmount.add(blockedClientAmount.getValue().abs());

        if (earmark != null) {
            newAmount = newAmount.subtract(earmark.getAmount().abs());
        }
        return newAmount;
    }
}
