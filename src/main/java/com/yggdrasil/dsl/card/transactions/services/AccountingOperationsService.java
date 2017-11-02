package com.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.FeeSchema;

import java.math.BigDecimal;

public class AccountingOperationsService {

    //todo: logs!
    //todo: move to service
    public BigDecimal calculateWirecardAmount(BigDecimal appliedAmount, BigDecimal amountToApply){
        if (appliedAmount == null) return amountToApply;
        return amountToApply.subtract(appliedAmount);
    }

    public BigDecimal calculateBlockedClientAmount(BigDecimal appliedAmount, BigDecimal amountToApply, FeeSchema feeSchema){
        BigDecimal clientFees = getFeeAmount(feeSchema, amountToApply);
        BigDecimal amountWithFees = amountToApply.add(clientFees).negate();
        if (appliedAmount == null) return amountWithFees;

        BigDecimal clientAmount =  appliedAmount.negate().add(amountWithFees);
        return clientAmount;
    }

    public BigDecimal calculateFees(BigDecimal appliedFees, BigDecimal currentFees){
        if (appliedFees == null) return currentFees;
        return currentFees.subtract(appliedFees);
    }

    public BigDecimal getFeeAmount(FeeSchema schema, BigDecimal settlementAmount){
        BigDecimal feeAmount = schema.getAmount();
        BigDecimal feePercentage = schema.getPercentage();
        //todo: ?
        return feeAmount;
    }

}
