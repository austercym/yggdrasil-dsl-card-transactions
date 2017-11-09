package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendGroup;
import com.orwellg.yggdrasil.dsl.card.transactions.model.AuthorisationMessage;

import java.math.BigDecimal;
import java.util.*;

public class GpsMessageMapper {

    private final List<Integer> historicalCurrencyNumericCodes = Arrays.asList(0, 642, 891);
    private final CardPresenceResolver cardPresenceResolver;
    private final TransactionTypeResolver transactionTypeResolver;
    private final Map<String, String> availableCurrencies;

    public GpsMessageMapper() {
        cardPresenceResolver = new CardPresenceResolver();
        transactionTypeResolver = new TransactionTypeResolver();
        availableCurrencies = createAvailableCurrencies();
//        availableCurrencies = Currency.getAvailableCurrencies().stream()
//                .filter(c -> !historicalCurrencyNumericCodes.contains(c.getNumericCode()))
//                .collect(Collectors.toMap(c -> Integer.toString(c.getNumericCode()), Currency::getCurrencyCode));
    }

    private Map<String, String> createAvailableCurrencies() {
        // TODO: What with duplicated numeric codes? E.g. RON 946
        HashMap<String, String> map = new HashMap<>();
        Currency.getAvailableCurrencies().forEach(c -> {
            String num = Integer.toString(c.getNumericCode());
            if (!historicalCurrencyNumericCodes.contains(c.getNumericCode())) {
                map.put(num, c.getSymbol());
            }
        });
        return map;
    }

    public AuthorisationMessage map(Message message) {
        AuthorisationMessage model = new AuthorisationMessage();
        model.setOriginalMessage(message);
        if (message.getCustRef() != null && !message.getCustRef().isEmpty())
            model.setDebitCardId(Long.parseLong(message.getCustRef()));
        model.setSpendGroup(getSpendGroup(message));
        if (message.getSettleAmt() != null)
            model.setSettlementAmount(BigDecimal.valueOf(message.getSettleAmt()));  // TODO: abs and add a credit/debit field
        model.setSettlementCurrency(currencyFromNumericCode(message.getSettleCcy()));
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
        return model;
    }

    private String currencyFromNumericCode(String numericCurrencyCode) {
        return availableCurrencies.get(numericCurrencyCode);
    }

    private SpendGroup getSpendGroup(Message message) {
        return MerchantCategoryCode.ATM.equals(message.getMCCCode())
                ? SpendGroup.ATM
                : SpendGroup.POS;
    }
}
