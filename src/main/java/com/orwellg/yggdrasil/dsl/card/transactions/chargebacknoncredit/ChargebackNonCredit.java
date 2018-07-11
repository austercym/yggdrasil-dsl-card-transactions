package com.orwellg.yggdrasil.dsl.card.transactions.chargebacknoncredit;

import com.orwellg.yggdrasil.card.transaction.commons.utils.TopologySubmitter;

public class ChargebackNonCredit {

    public static void main(String[] args) throws Exception {
        TopologySubmitter.submit(ChargebackNonCreditTopology::new, ChargebackNonCreditTopology.PROPERTIES_FILE, args);
    }
}
