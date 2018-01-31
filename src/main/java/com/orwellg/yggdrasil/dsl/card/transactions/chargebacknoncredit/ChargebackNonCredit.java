package com.orwellg.yggdrasil.dsl.card.transactions.chargebacknoncredit;

import com.orwellg.yggdrasil.dsl.card.transactions.common.TopologyLoader;

public class ChargebackNonCredit {

    public static void main(String[] args) throws Exception {
        TopologyLoader.submitTopology(ChargebackNonCreditTopology::new, ChargebackNonCreditTopology.PROPERTIES_FILE, args);
    }
}
