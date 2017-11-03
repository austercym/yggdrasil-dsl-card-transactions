package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.Message;

public class CardPresenceResolver {

    private static final String CARD_PRESENT = "0";
    private static final Integer POS_CARD_PRESENCE_SUB_FIELD = 5;

    public Boolean isCardPresent(Message message) {
        return CARD_PRESENT.equals(getPosCardPresence(message.getPOSDataDE61()));
    }

    private String getPosCardPresence(String posDataDE61) {
        return posDataDE61 == null || posDataDE61.length() < POS_CARD_PRESENCE_SUB_FIELD
                ? null
                : posDataDE61.substring(POS_CARD_PRESENCE_SUB_FIELD - 1, POS_CARD_PRESENCE_SUB_FIELD);
    }
}
