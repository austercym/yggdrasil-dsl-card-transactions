package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.FeeSchema;
import com.orwellg.yggdrasil.dsl.card.transactions.model.GpsMessageProcessingException;
import com.orwellg.yggdrasil.dsl.card.transactions.model.PresentmentErrorCode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;

public class AccountingOperationsService {

    private final static Logger LOG = LogManager.getLogger(AccountingOperationsService.class);

    public BigDecimal getFeeAmount(FeeSchema schema, BigDecimal settlementAmount){

        if (settlementAmount == null){
            //todo: throw exception?
        }

        BigDecimal percentageFee = BigDecimal.valueOf(0);

        if (schema.getPercentage() != null)
            percentageFee =  schema.getPercentage().multiply(settlementAmount).divide(BigDecimal.valueOf(100.0));

        BigDecimal amountFee = schema.getAmount();
        BigDecimal result = percentageFee.add(amountFee);

        LOG.debug("fee amount calculated. percentage: {}, percentage fee: {}, settlementAmount:{}, amount fee: {}, result: {} ", schema.getPercentage(),
                percentageFee, settlementAmount, amountFee, result);

        return result;
    }

    public BigDecimal calculateBlockedClientAmount(BigDecimal settlementAmount, BigDecimal feeAmount){

        if (feeAmount == null | settlementAmount == null){
            //todo: throw error


        }
        return settlementAmount.add(feeAmount).negate();
    }
}
