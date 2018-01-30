package com.orwellg.yggdrasil.dsl.card.transactions.presentment.services;

import com.orwellg.umbrella.avro.types.cards.SpendGroup;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionType;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.yggdrasil.dsl.card.transactions.model.PresentmentMessage;

public class ResponseService {


    public GpsMessageProcessed generateResponse(PresentmentMessage presentment){

        GpsMessageProcessed gpsMessageProcessed = new GpsMessageProcessed();
        gpsMessageProcessed.setGpsMessageType(presentment.getGpsMessageType());
        gpsMessageProcessed.setGpsTransactionLink(presentment.getGpsTransactionLink());
        gpsMessageProcessed.setGpsTransactionId(presentment.getGpsTransactionId());
        gpsMessageProcessed.setDebitCardId(presentment.getDebitCardId());
        gpsMessageProcessed.setTransactionTimestamp(presentment.getTransactionTimestamp().getTime()); //todo: helper to
        gpsMessageProcessed.setGpsTransactionTime(presentment.getGpsTrnasactionDate().getTime());
        gpsMessageProcessed.setInternalAccountId(presentment.getInternalAccountId());
        gpsMessageProcessed.setInternalAccountCurrency(presentment.getInternalAccountCurrency());
        gpsMessageProcessed.setSpendGroup(presentment.getTransactionType() == TransactionType.ATM
                    ? SpendGroup.ATM
                    : SpendGroup.POS);

        gpsMessageProcessed.setWirecardAmount(DecimalTypeUtils.toDecimal(presentment.getSettlementAmount()));
        gpsMessageProcessed.setWirecardCurrency(presentment.getSettlementCurrency());
        gpsMessageProcessed.setClientAmount(DecimalTypeUtils.toDecimal(presentment.getBlockedClientAmount().negate()));
        gpsMessageProcessed.setClientCurrency(presentment.getSettlementCurrency());
        gpsMessageProcessed.setFeesAmount(DecimalTypeUtils.toDecimal(presentment.getFeeAmount()));
        gpsMessageProcessed.setFeesCurrency(presentment.getInternalAccountCurrency());

        double authBlockedAmount = presentment.getAuthBlockedClientAmount() == null ? 0.0 : presentment.getAuthBlockedClientAmount().doubleValue();
//        gpsMessageProcessed.setAppliedBlockedClientAmount(DecimalTypeUtils.toDecimal(authBlockedAmount));
//        gpsMessageProcessed.setAppliedBlockedClientCurrency(presentment.getAuthBlockedClientCurrency());
        double authWirecardAmount = presentment.getAuthWirecardAmount() == null ? 0.0 : presentment.getAuthWirecardAmount().doubleValue();
        gpsMessageProcessed.setTotalWirecardAmount(DecimalTypeUtils.toDecimal(authWirecardAmount));
        gpsMessageProcessed.setTotalWirecardCurrency(presentment.getAuthWirecardCurrency());
        double authFeeAmount = presentment.getAuthFeeAmount() == null ? 0.0 : presentment.getAuthFeeAmount().doubleValue();
        gpsMessageProcessed.setTotalFeesAmount(DecimalTypeUtils.toDecimal(authFeeAmount));
        gpsMessageProcessed.setTotalFeesCurrency(presentment.getAuthFeeCurrency());

        return gpsMessageProcessed;
    }

}
