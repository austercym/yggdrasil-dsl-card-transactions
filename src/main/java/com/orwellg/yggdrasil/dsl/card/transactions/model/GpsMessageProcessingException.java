package com.orwellg.yggdrasil.dsl.card.transactions.model;

import org.apache.log4j.spi.ErrorCode;

public class GpsMessageProcessingException extends Exception {


    public GpsMessageProcessingException(PresentmentErrorCode presentmentCode, String message) {
        super(presentmentCode + "|" +  presentmentCode.getNumber() + "|" + "|" + message);
    }

    public GpsMessageProcessingException(PresentmentErrorCode presentmentCode) {
        super(presentmentCode.toString());
    }

}
