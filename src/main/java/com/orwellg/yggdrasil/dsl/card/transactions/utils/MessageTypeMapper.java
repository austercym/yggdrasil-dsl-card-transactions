package com.orwellg.yggdrasil.dsl.card.transactions.utils;

import com.orwellg.umbrella.avro.types.cards.MessageType;
import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import org.apache.commons.lang3.Validate;

public final class MessageTypeMapper {

    private static BidiMap<String, MessageType> gpsMap = new DualHashBidiMap<String, MessageType>() {
        {
            put("A", MessageType.AUTHORISATION);
            put("C", MessageType.CHARGEBACK);
            put("D", MessageType.AUTH_REVERSAL);
            put("E", MessageType.FINANCIAL_REVERSAL);
            put("H", MessageType.CHARGEBACK_NON_CREDIT);
            put("N", MessageType.SEC_PRESENTMENT);
            put("P", MessageType.PRESENTMENT);
            put("Y", MessageType.CARD_EXPIRY);
        }
    };

    public static MessageType fromGpsTxnType(String txnType) {
        Validate.notBlank(txnType, "txnType cannot be null or empty");
        return gpsMap.get(txnType.toUpperCase());
    }

    public static String toGpsTxnType(MessageType messageType) {
        Validate.notNull(messageType, "messageType cannot be null");
        return gpsMap.getKey(messageType);
    }
}
