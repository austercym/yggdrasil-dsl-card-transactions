package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import org.apache.commons.lang3.ObjectUtils;
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

    public SpendingTotalAmounts recalculate(MessageProcessed messageProcessed, SpendingTotalAmounts lastSpendingTotalAmounts, CardTransaction authorisation) {

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
        BigDecimal clientAmountWithEarmark =
                ObjectUtils.firstNonNull(messageProcessed.getClientAmount(), DecimalTypeUtils.toDecimal(0)).getValue().add(
                        ObjectUtils.firstNonNull(messageProcessed.getEarmarkAmount(), DecimalTypeUtils.toDecimal(0)).getValue());

        if (clientAmountWithEarmark.compareTo(BigDecimal.ZERO) == 0) {
            LOG.info(
                    "Spending total amounts will not be updated - transaction's client and earmark amounts are zero for ProviderTransactionId={}",
                    messageProcessed.getProviderTransactionId());
        } else if (clientAmountWithEarmark.compareTo(BigDecimal.ZERO) > 0) {
            LOG.info(
                    "Spending total amounts will not be updated - transaction's sum of client and earmark amounts is positive ProviderTransactionId={}",
                    messageProcessed.getProviderTransactionId());
        } else {
            if (transactionTimestamp.toLocalDate().equals(today)) {
                daily = calculateNewAmount(daily, clientAmountWithEarmark, authorisation);
            } else {
                LOG.info(
                        "Daily spending total amount will not be updated - ProviderTransactionId={}, TransactionTimestamp={}",
                        messageProcessed.getProviderTransactionId(), transactionTimestamp);
            }

            if (transactionTimestamp.getYear() == today.getYear()) {
                annual = calculateNewAmount(annual, clientAmountWithEarmark, authorisation);
            } else {
                LOG.info(
                        "Annual spending total amount will not be updated - ProviderTransactionId={}, TransactionTimestamp={}",
                        messageProcessed.getProviderTransactionId(), transactionTimestamp);
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

    private BigDecimal calculateNewAmount(BigDecimal currentAmount, BigDecimal blockedClientAmount, CardTransaction authorisation) {
        BigDecimal newAmount = currentAmount.add(blockedClientAmount.abs());

        if (authorisation != null) {
            newAmount = newAmount.subtract(authorisation.getEarmarkAmount().abs());
        }
        return newAmount;
    }
}
