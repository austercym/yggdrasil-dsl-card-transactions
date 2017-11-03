package com.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionType;
import com.yggdrasil.dsl.card.transactions.FeeTransactionType;
import com.yggdrasil.dsl.card.transactions.GpsMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class ParseMessageService {

    private final static Logger LOG = LogManager.getLogger(AccountingOperationsService.class);
    private TransactionTypeResolver transactionTypeResolver;

    private final static String chargebackGpsMessageType = "C";
    private final static String balanceEnquireCode = "30";
    private final static String withdrawalCode = "01";

    public ParseMessageService(){
        transactionTypeResolver = new TransactionTypeResolver();
    }

    public GpsMessage parse(Message message) throws ParseException {

        LOG.info("parsing message: {}", message);

        TransactionType transactionType = transactionTypeResolver.getType(message);
        FeeTransactionType feeTransactionType = getTransactionType(message.getTxnType(), message.getTxnCtry(), transactionType, message.getProcCode());
        Date gpsTransactionDateOrUtcNow = getGpsTransactionDate(message.getTxnGPSDate());
        Date transactionTimestamp = getTransactionTimestamp(message.getPOSTimeDE12(), message.getTxnCtry(), message.getTXNTimeDE07(), gpsTransactionDateOrUtcNow);
        BigDecimal settlementAmount = BigDecimal.valueOf(message.getSettleAmt()).negate();

        GpsMessage transaction = new GpsMessage();
        transaction.setDebitCardId(Long.parseLong(message.getCustRef()));
        transaction.setGpsTransactionId(message.getTXnID());
        transaction.setGpsTransactionLink(message.getTransLink());
        transaction.setGpsTrnasactionDate(gpsTransactionDateOrUtcNow);
        transaction.setGpsMessageType(message.getTxnType());
        transaction.setTransactionType(transactionType);
        transaction.setFeeTransactionType(feeTransactionType);
        transaction.setTransactionTimestamp(transactionTimestamp);
        transaction.setSettlementAmount(settlementAmount);
        transaction.setSettlementCurrency(message.getSettleCcy());

        LOG.info("parsing message completed: {}", message);

        return transaction;
    }

    public FeeTransactionType getTransactionType(String gpsMessageType, String transactionCountryCode, TransactionType transactionType, String procCode){

        if(gpsMessageType.equals(chargebackGpsMessageType)) return FeeTransactionType.ChargeBack;
        if(transactionType == TransactionType.ATM){
            //todo: check if EU country?
            if (procCode.equals(balanceEnquireCode)) return FeeTransactionType.ATMBalanceEnquiries;
            if (procCode.equals(withdrawalCode)) return FeeTransactionType.ATMWithdrawalsEU;
            //todo: what if something else?
        }
        if (transactionType == TransactionType.POS){
            return FeeTransactionType.POSTransactionsEU;
            //return FeeTransactionType.ATMWithdrawalsEU;
        }
        if (transactionType == TransactionType.ONLINE){
            return FeeTransactionType.OnlineTransactionsEU;
        }

        return null;
    }

    private Date getGpsTransactionDate(String gpsTransactionTime){

        Date gpsDateOrUtcNow = new Date();

        try{
            if (!gpsTransactionTime.isEmpty() && gpsTransactionTime != null){
                SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");
                gpsDateOrUtcNow = parser.parse(gpsTransactionTime);
            }
        } catch (ParseException e) {
            LOG.error("Parsing gpsTransactionDateTime failed - using current utc time. gpsTransactionTime: {}. Error: {}", gpsTransactionTime, e);
        }
        return gpsDateOrUtcNow;
    }

    private Date getTransactionTimestamp(String postimede12, String countryCode, String txntime07, Date gpsTransactionTime) throws ParseException {

        Calendar gpsCalendarOtUtc = Calendar.getInstance();
        gpsCalendarOtUtc.setTime(gpsTransactionTime);

            //todo: this is in local time!
            //need to convert it to utc time zone using txn ctry code
            //135703
            //yyMMddHHmmss
            if (!postimede12.isEmpty() && postimede12 != null && postimede12.length() == 12) {
                SimpleDateFormat parser = new SimpleDateFormat("yyMMddHHmmss");
                Date date = parser.parse(postimede12);
                return date;
            }

            //this is in utc
            //0912115703
            if (!txntime07.isEmpty() && txntime07 != null) {
                //get year from gpsDate
                int year = gpsCalendarOtUtc.get(Calendar.YEAR);
                String fulldate = "" + year + txntime07;
                SimpleDateFormat parser = new SimpleDateFormat("yyyyMMddHHmmss");
                Date date = parser.parse(fulldate);
                return date;
            }

            return gpsTransactionTime;
    }

}

