package com.orwellg.yggdrasil.dsl.card.transactions.model;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionType;


import java.math.BigDecimal;
import java.util.Date;

public class PresentmentMessage {

    private Long InternalAccountId;
    private String InternalAccountCurrency;
    private String GpsTransactionId;
    private String GpsTransactionLink;
    private Date GpsTrnasactionDate;
    private Long DebitCardId;
    private Date TransactionTimestamp;
    private BigDecimal SettlementAmount;
    private String SettlementCurrency;
    private String GpsMessageType;
    private TransactionType TransactionType;
    private com.orwellg.yggdrasil.dsl.card.transactions.model.FeeTransactionType FeeTransactionType;
    private BigDecimal AuthWirecardAmount;
    private String AuthWirecardCurrency;
    private BigDecimal AuthBlockedClientAmount;
    private String AuthBlockedClientCurrency;
    private BigDecimal AuthFeeAmount;
    private String AuthFeeCurrency;
    private BigDecimal BlockedClientAmount;
    private String BlockedClientCurrency;
    private BigDecimal FeeAmount;
    private String FeeCurrency;


    public PresentmentMessage UpdateWithAuthorisationData(CardTransaction lastTransaction){

        if (lastTransaction != null){
            this.setAuthBlockedClientAmaount(lastTransaction.getBlockedClientAmount());
            this.setAuthBlockedClientCurrency(lastTransaction.getBlockedClientCurrency());
            this.setAuthWirecardAmount(lastTransaction.getWirecardAmount());
            this.setAuthWirecardCurrency(lastTransaction.getWirecardCurrency());
            this.setInternalAccountId(lastTransaction.getInternalAccountId());
            this.setInternalAccountCurrency(lastTransaction.getInternalAccountCurrency()); //todo:??
            this.setAuthFeeAmount(lastTransaction.getFeeAmount());
            this.setAuthFeeCurrency(lastTransaction.getInternalAccountCurrency()); //todo: what currency should we get fees?
        }
        return this;
    }

    public BigDecimal getFeeAmount() {
        return FeeAmount;
    }

    public void setFeeAmount(BigDecimal feeAmount) {
        FeeAmount = feeAmount;
    }

    public String getFeeCurrency() {
        return FeeCurrency;
    }

    public void setFeeCurrency(String feeCurrency) {
        FeeCurrency = feeCurrency;
    }

    public BigDecimal getBlockedClientAmount() {
        return BlockedClientAmount;
    }

    public void setBlockedClientAmount(BigDecimal blockedClientAmount) {
        BlockedClientAmount = blockedClientAmount;
    }


    public BigDecimal getAuthBlockedClientAmount() {
        return AuthBlockedClientAmount;
    }

    public void setAuthBlockedClientAmount(BigDecimal authBlockedClientAmount) {
        AuthBlockedClientAmount = authBlockedClientAmount;
    }

    public String getBlockedClientCurrency() {
        return BlockedClientCurrency;
    }

    public void setBlockedClientCurrency(String blockedClientCurrency) {
        BlockedClientCurrency = blockedClientCurrency;
    }

    public com.orwellg.yggdrasil.dsl.card.transactions.model.FeeTransactionType getFeeTransactionType() {
        return FeeTransactionType;
    }

    public void setFeeTransactionType(com.orwellg.yggdrasil.dsl.card.transactions.model.FeeTransactionType feeTransactionType) {
        FeeTransactionType = feeTransactionType;
    }

    public Long getInternalAccountId() {
        return InternalAccountId;
    }

    public void setInternalAccountId(Long internalAccountId) {
        InternalAccountId = internalAccountId;
    }

    public String getInternalAccountCurrency() {
        return InternalAccountCurrency;
    }

    public void setInternalAccountCurrency(String internalAccountCurrency) {
        InternalAccountCurrency = internalAccountCurrency;
    }

    public String getGpsTransactionId() {
        return GpsTransactionId;
    }

    public void setGpsTransactionId(String gpsTransactionId) {
        GpsTransactionId = gpsTransactionId;
    }

    public String getGpsTransactionLink() {
        return GpsTransactionLink;
    }

    public void setGpsTransactionLink(String gpsTransactionLink) {
        GpsTransactionLink = gpsTransactionLink;
    }

    public Date getGpsTrnasactionDate() {
        return GpsTrnasactionDate;
    }

    public void setGpsTrnasactionDate(Date gpsTrnasactionDate) {
        GpsTrnasactionDate = gpsTrnasactionDate;
    }

    public Long getDebitCardId() {
        return DebitCardId;
    }

    public void setDebitCardId(Long debitCardId) {
        DebitCardId = debitCardId;
    }

    public Date getTransactionTimestamp() {
        return TransactionTimestamp;
    }

    public void setTransactionTimestamp(Date transactionTimestamp) {
        TransactionTimestamp = transactionTimestamp;
    }

    public BigDecimal getSettlementAmount() {
        return SettlementAmount;
    }

    public void setSettlementAmount(BigDecimal settlementAmount) {
        SettlementAmount = settlementAmount;
    }

    public String getSettlementCurrency() {
        return SettlementCurrency;
    }

    public void setSettlementCurrency(String settlementCurrency) {
        SettlementCurrency = settlementCurrency;
    }

    public String getGpsMessageType() {
        return GpsMessageType;
    }

    public void setGpsMessageType(String gpsMessageType) {
        GpsMessageType = gpsMessageType;
    }

    public TransactionType getTransactionType() {
        return TransactionType;
    }

    public void setTransactionType(TransactionType transactionType) {
        TransactionType = transactionType;
    }

    public BigDecimal getAuthWirecardAmount() {
        return AuthWirecardAmount;
    }

    public void setAuthWirecardAmount(BigDecimal authWirecardAmount) {
        AuthWirecardAmount = authWirecardAmount;
    }

    public String getAuthWirecardCurrency() {
        return AuthWirecardCurrency;
    }

    public void setAuthWirecardCurrency(String authWirecardCurrency) {
        AuthWirecardCurrency = authWirecardCurrency;
    }


    public void setAuthBlockedClientAmaount(BigDecimal authBlockedClientAmaount) {
        AuthBlockedClientAmount = authBlockedClientAmaount;
    }

    public String getAuthBlockedClientCurrency() {
        return AuthBlockedClientCurrency;
    }

    public void setAuthBlockedClientCurrency(String authBlockedClientCurrency) {
        AuthBlockedClientCurrency = authBlockedClientCurrency;
    }

    public BigDecimal getAuthFeeAmount() {
        return AuthFeeAmount;
    }

    public void setAuthFeeAmount(BigDecimal authFeeAmount) {
        AuthFeeAmount = authFeeAmount;
    }

    public String getAuthFeeCurrency() {
        return AuthFeeCurrency;
    }

    public void setAuthFeeCurrency(String authFeeCurrency) {
        AuthFeeCurrency = authFeeCurrency;
    }



}
