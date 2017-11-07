package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendGroup;
import com.orwellg.yggdrasil.dsl.card.transactions.model.AuthorisationMessage;

import java.math.BigDecimal;

public class GpsMessageMapper {

    private final CardPresenceResolver cardPresenceResolver;
    private final TransactionTypeResolver transactionTypeResolver;

    public GpsMessageMapper() {
        cardPresenceResolver = new CardPresenceResolver();
        transactionTypeResolver = new TransactionTypeResolver();
    }

    public AuthorisationMessage map(Message message){
        AuthorisationMessage model = new AuthorisationMessage();
        model.setOriginalMessage(message);
        model.setDebitCardId(Long.parseLong(message.getCustRef()));
        model.setSpendGroup(getSpendGroup(message));
        model.setSettlementAmount(BigDecimal.valueOf(message.getSettleAmt()));  // TODO: abs and add a credit/debit field
        model.setSettlementCurrency(message.getSettleCcy());    // TODO: map currency codes
        model.setIsCardPresent(cardPresenceResolver.isCardPresent(message));
        model.setMerchantId(message.getMerchIDDE42() == null ? null : message.getMerchIDDE42().trim());
        model.setTransactionType(transactionTypeResolver.getType(message));
        model.setGpsTransactionLink(message.getTransLink());
        model.setGpsTransactionId(message.getTXnID());
        model.setCardToken(message.getToken());
        model.setTransactionAmount(BigDecimal.valueOf(message.getTxnAmt()));
        model.setTransactionCurrency(message.getTxnCCy());  // TODO: map currency codes
        return model;
    }

    private SpendGroup getSpendGroup(Message message) {
        return Mcc.ATM.equals(message.getMCCCode())
                ? SpendGroup.ATM
                : SpendGroup.POS;
    }
}
