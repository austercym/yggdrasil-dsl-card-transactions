package com.orwellg.yggdrasil.dsl.card.transactions.model;

public class GpsMessageProcessingException extends Exception {


    public GpsMessageProcessingException(PresentmentErrorCode presentmentCode, String message) {
        super(presentmentCode + "|" +  presentmentCode.getNumber() + "|" + "|" + message);
    }

    public GpsMessageProcessingException(PresentmentErrorCode presentmentCode) {
        super(presentmentCode.toString());
    }

}
