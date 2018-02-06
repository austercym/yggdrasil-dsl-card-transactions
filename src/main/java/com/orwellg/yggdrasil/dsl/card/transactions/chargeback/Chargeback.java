package com.orwellg.yggdrasil.dsl.card.transactions.chargeback;

import com.orwellg.yggdrasil.dsl.card.transactions.common.TopologySubmitter;

public class Chargeback {

    public static void main(String[] args) throws Exception {
        TopologySubmitter.submit(ChargebackTopology::new, ChargebackTopology.PROPERTIES_FILE, args);
    }
}
