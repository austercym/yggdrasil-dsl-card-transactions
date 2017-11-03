package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.yggdrasil.dsl.card.transactions.GpsMessage;

import java.math.BigDecimal;
import java.util.Date;

public class ParseMessageService {

    public GpsMessage parse(Message message){

        GpsMessage transaction = new GpsMessage();
        transaction.setDebitCardId(Long.parseLong(message.getCustRef()));
        transaction.setGpsTransactionId(message.getTXnID());
        transaction.setGpsTransactionLink(message.getTransLink());
        transaction.setGpsMessageType(message.getTxnType());

        //todo: ! get the transaction type
        transaction.setTransactionType("OnlineTransactionsEU");

        Long timestamp = Long.parseLong(message.getPOSTimeDE12());
        Date date = new Date((long)timestamp*1000);
        transaction.setTransactionTimestamp(date);

        //BigDecimal settlementAmount = new BigDecimal(message.getSettleAmt(), MathContext.DECIMAL128);
        BigDecimal settlementAmount = BigDecimal.valueOf(message.getSettleAmt()).negate();
        transaction.setSettlementAmount(settlementAmount);
        transaction.setSettlementCurrency(message.getSettleCcy());
        //transaction.setOriginalWirecardAmount(settlementAmount.doubleValue());
        //transaction.setOriginalBlockedClientAmount(-settlementAmount.doubleValue());

        //POS_Time_DE12 or TXN_Time_DE07

        return transaction;
    }

}
