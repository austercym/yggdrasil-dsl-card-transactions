package com.orwellg.yggdrasil.dsl.card.transactions.chargeback;

import com.orwellg.yggdrasil.dsl.card.transactions.common.TopologyLoader;

public class Chargeback {

    public static void main(String[] args) throws Exception {
        TopologyLoader.submitTopology(ChargebackTopology::new, ChargebackTopology.PROPERTIES_FILE, args);
    }
}
