package com.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionType;

public class TransactionTypeResolver {

    private static final String ELECTRONIC_ORDER = "5";
    private static final Integer POS_CARDHOLDER_PRESENCE_SUB_FIELD = 4;

    public TransactionType getType(Message message) {
        if (Mcc.ATM.equals(message.getMCCCode())) {
            return TransactionType.ATM;
        }
        if (ELECTRONIC_ORDER.equals(getPosCardholderPresence(message.getPOSDataDE61()))) {
            return TransactionType.ONLINE;
        }
        return TransactionType.POS;
    }

    private String getPosCardholderPresence(String posDataDE61) {
        return posDataDE61 == null || posDataDE61.length() < POS_CARDHOLDER_PRESENCE_SUB_FIELD
                ? null
                : posDataDE61.substring(POS_CARDHOLDER_PRESENCE_SUB_FIELD - 1, POS_CARDHOLDER_PRESENCE_SUB_FIELD);
    }
}
