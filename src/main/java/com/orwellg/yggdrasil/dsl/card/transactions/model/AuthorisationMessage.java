package com.orwellg.yggdrasil.dsl.card.transactions.model;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendGroup;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionType;

import java.io.Serializable;
import java.math.BigDecimal;

public class AuthorisationMessage implements Serializable {
    private Message originalMessage;
    private Long debitCardId;
    private SpendGroup spendGroup;
    private BigDecimal settlementAmount;
    private Boolean isCardPresent;
    private String merchantId;
    private TransactionType transactionType;
    private String settlementCurrency;
    private String gpsTransactionLink;
    private String gpsTransactionId;
    private String cardToken;
    private BigDecimal transactionAmount;
    private String transactionCurrency;
    private CreditDebit creditDebit;

    public Message getOriginalMessage() {
        return originalMessage;
    }

    public void setOriginalMessage(Message originalMessage) {
        this.originalMessage = originalMessage;
    }

    public Long getDebitCardId() {
        return debitCardId;
    }

    public void setDebitCardId(Long debitCardId) {
        this.debitCardId = debitCardId;
    }

    public SpendGroup getSpendGroup() {
        return spendGroup;
    }

    public void setSpendGroup(SpendGroup totalSpendGroup) {
        this.spendGroup = totalSpendGroup;
    }

    public BigDecimal getSettlementAmount() {
        return settlementAmount;
    }

    public void setSettlementAmount(BigDecimal settlementAmount) {
        this.settlementAmount = settlementAmount;
    }

    public Boolean getIsCardPresent() {
        return isCardPresent;
    }

    public void setIsCardPresent(Boolean isCardPresent) {
        this.isCardPresent = isCardPresent;
    }

    public String getMerchantId() {
        return merchantId;
    }

    public void setMerchantId(String merchantId) {
        this.merchantId = merchantId;
    }

    public TransactionType getTransactionType() {
        return transactionType;
    }

    public void setTransactionType(TransactionType transactionType) {
        this.transactionType = transactionType;
    }

    public String getSettlementCurrency() {
        return settlementCurrency;
    }

    public void setSettlementCurrency(String settlementCurrency) {
        this.settlementCurrency = settlementCurrency;
    }

    public void setGpsTransactionLink(String gpsTransactionLink) {
        this.gpsTransactionLink = gpsTransactionLink;
    }

    public String getGpsTransactionLink() {
        return gpsTransactionLink;
    }

    public void setGpsTransactionId(String gpsTransactionId) {
        this.gpsTransactionId = gpsTransactionId;
    }

    public String getGpsTransactionId() {
        return gpsTransactionId;
    }

    public void setCardToken(String cardToken) {
        this.cardToken = cardToken;
    }

    public String getCardToken() {
        return cardToken;
    }

    public void setTransactionAmount(BigDecimal transactionAmount) {
        this.transactionAmount = transactionAmount;
    }

    public BigDecimal getTransactionAmount() {
        return transactionAmount;
    }

    public void setTransactionCurrency(String transactionCurrency) {
        this.transactionCurrency = transactionCurrency;
    }

    public String getTransactionCurrency() {
        return transactionCurrency;
    }

    public CreditDebit getCreditDebit() {
        return creditDebit;
    }

    public void setCreditDebit(CreditDebit creditDebit) {
        this.creditDebit = creditDebit;
    }
}
