package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.cards.SpendGroup;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.yggdrasil.dsl.card.transactions.model.CreditDebit;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Currency;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GpsMessageMapper {

    static final String BALANCE_INQUIRY_SERVICE = "30";
    private final CardPresenceResolver cardPresenceResolver;
    private final TransactionTypeResolver transactionTypeResolver;
    private final Map<String, String> availableCurrencies;
    private List<String> historicalCurrencyCurrencyCodes = Arrays.asList("YUM", "ROL", "CSD", "XFU", "XFO");

    public GpsMessageMapper() {
        cardPresenceResolver = new CardPresenceResolver();
        transactionTypeResolver = new TransactionTypeResolver();
        availableCurrencies = Currency.getAvailableCurrencies().stream()
                .filter(c -> !historicalCurrencyCurrencyCodes.contains(c.getCurrencyCode()))
                .collect(Collectors.toMap(c -> Integer.toString(c.getNumericCode()), Currency::getCurrencyCode));
    }

    public TransactionInfo map(Message message) {
        TransactionInfo model = new TransactionInfo();
        model.setMessage(message);
        if (message.getCustRef() != null && !message.getCustRef().isEmpty())
            model.setDebitCardId(Long.parseLong(message.getCustRef()));
        model.setSpendGroup(getSpendGroup(message));
        Double settlementBillingAmount = ObjectUtils.firstNonNull(message.getBillAmt(), message.getSettleAmt());
        if (settlementBillingAmount != null) {
            model.setSettlementAmount(BigDecimal.valueOf(settlementBillingAmount));
            if (settlementBillingAmount > 0)
                model.setCreditDebit(CreditDebit.CREDIT);
            else if (settlementBillingAmount < 0)
                model.setCreditDebit(CreditDebit.DEBIT);
        }
        model.setSettlementCurrency(currencyFromNumericCode(ObjectUtils.firstNonNull(
                message.getBillCcy(), message.getSettleCcy())));
        model.setIsCardPresent(cardPresenceResolver.isCardPresent(message));
        if (message.getMerchIDDE42() != null)
            model.setMerchantId(message.getMerchIDDE42().trim());
        model.setTransactionType(transactionTypeResolver.getType(message));
        model.setGpsTransactionLink(message.getTransLink());
        model.setGpsTransactionId(message.getTXnID());
        model.setCardToken(message.getToken());
        if (message.getTxnAmt() != null)
            model.setTransactionAmount(BigDecimal.valueOf(message.getTxnAmt()));
        model.setTransactionCurrency(currencyFromNumericCode(message.getTxnCCy()));
        model.setIsBalanceEnquiry(isBalanceEnquiry(message.getProcCode()));
        if (StringUtils.isNotBlank(message.getTxnGPSDate())) {
            model.setGpsTransactionTime(parseDateTime(message.getTxnGPSDate()));
        }
        return model;
    }

    private LocalDateTime parseDateTime(String dateTime) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
        return LocalDateTime.parse(dateTime, formatter);
    }

    private String currencyFromNumericCode(String numericCurrencyCode) {
        return availableCurrencies.get(numericCurrencyCode);
    }

    private SpendGroup getSpendGroup(Message message) {
        return MerchantCategoryCode.ATM.equals(message.getMCCCode())
                ? SpendGroup.ATM
                : SpendGroup.POS;
    }

    private Boolean isBalanceEnquiry(String procCode){
        return procCode != null && procCode.startsWith(BALANCE_INQUIRY_SERVICE);
    }
}
