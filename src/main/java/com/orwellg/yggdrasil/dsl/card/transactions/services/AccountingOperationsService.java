package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.FeeSchema;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;

public class AccountingOperationsService {

    private final static Logger LOG = LogManager.getLogger(AccountingOperationsService.class);


    public BigDecimal calculateWirecardAmount(BigDecimal appliedAmount, BigDecimal amountToApply){
        if (appliedAmount == null) {
            LOG.debug("Wirecard amount calculated. AppliedAmount: {}, amountToApply:{}, result: {} ", appliedAmount, amountToApply, amountToApply);
            return amountToApply;
        }
        BigDecimal result = amountToApply.subtract(appliedAmount);
        LOG.debug("Wirecard amount calculated. AppliedAmount: {}, amountToApply:{}, result: {} ", appliedAmount, amountToApply, result);
        return result;
    }

    public BigDecimal calculateBlockedClientAmount(BigDecimal appliedAmount, BigDecimal amountToApply, FeeSchema feeSchema){
        BigDecimal clientFees = getFeeAmount(feeSchema, amountToApply);
        BigDecimal amountWithFees = amountToApply.add(clientFees).negate();
        if (appliedAmount == null) {
            LOG.debug("blocked client amount calculated. AppliedAmount: {}, amountToApply:{}, fees: {}, result: {} ", appliedAmount, amountToApply, clientFees, amountWithFees);
            return amountWithFees;
        }

        BigDecimal clientAmount =  appliedAmount.negate().add(amountWithFees);
        LOG.debug("blocked client amount calculated. AppliedAmount: {}, amountToApply:{}, fees: {}, result: {} ", appliedAmount, amountToApply, clientFees, clientAmount);
        return clientAmount;
    }

    public BigDecimal calculateFees(BigDecimal appliedFees, BigDecimal currentFees){
        if (appliedFees == null) return currentFees;
        return currentFees.subtract(appliedFees);
    }

    public BigDecimal getFeeAmount(FeeSchema schema, BigDecimal settlementAmount){
        BigDecimal percentageFee =  schema.getPercentage().multiply(settlementAmount).divide(BigDecimal.valueOf(100.0));
        BigDecimal amountFee = schema.getAmount();
        BigDecimal result = percentageFee.add(amountFee);
        LOG.debug("fee amount calculated. percentage: {}, percentage fee: {}, settlementAmount:{}, amount fee: {}, result: {} ", schema.getPercentage(),
                percentageFee, settlementAmount, amountFee, result);
        return result;
    }

}
