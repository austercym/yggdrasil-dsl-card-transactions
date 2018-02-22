package com.orwellg.yggdrasil.dsl.card.transactions.model;

import com.orwellg.umbrella.avro.types.cards.SpendGroup;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionType;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDateTime;

public class TransactionInfo implements Serializable {
    private Message message;
    private Long debitCardId;
    private SpendGroup spendGroup;
    private BigDecimal settlementAmount;
    private Boolean isCardPresent;
    private String merchantId;
    private TransactionType transactionType;
    private String settlementCurrency;
    private String gpsTransactionLink;
    private String gpsTransactionId;
    private LocalDateTime gpsTransactionTime;
    private String cardToken;
    private BigDecimal transactionAmount;
    private String transactionCurrency;
    private CreditDebit creditDebit;
    private Boolean isBalanceEnquiry;
    private LocalDateTime transactionDateTime;

    public Message getMessage() {
        return message;
    }

    public void setMessage(Message message) {
        this.message = message;
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

    public Boolean getIsBalanceEnquiry() {
        return isBalanceEnquiry;
    }

    public void setIsBalanceEnquiry(Boolean balanceEnquiry) {
        isBalanceEnquiry = balanceEnquiry;
    }

    public LocalDateTime getGpsTransactionTime() {
        return gpsTransactionTime;
    }

    public void setGpsTransactionTime(LocalDateTime gpsTransactionTime) {
        this.gpsTransactionTime = gpsTransactionTime;
    }

    public LocalDateTime getTransactionDateTime() {
        return transactionDateTime;
    }

    public void setTransactionDateTime(LocalDateTime transactionDateTime) {
        this.transactionDateTime = transactionDateTime;
    }

    @Override
    public String toString() {
        return "TransactionInfo{" +
                "gpsTransactionLink='" + gpsTransactionLink + '\'' +
                ", gpsTransactionId='" + gpsTransactionId + '\'' +
                ", debitCardId=" + debitCardId +
                ", transactionAmount=" + transactionAmount +
                ", transactionCurrency='" + transactionCurrency + '\'' +
                ", creditDebit=" + creditDebit +
                ", transactionType=" + transactionType +
                ", transactionDateTime=" + transactionDateTime +
                '}';
    }
}
