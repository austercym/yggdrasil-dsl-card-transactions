package com.orwellg.yggdrasil.dsl.card.transactions.presentment;

import com.orwellg.yggdrasil.dsl.card.transactions.common.TopologySubmitter;

public class Presentment {

    public static void main(String[] args) throws Exception {
        TopologySubmitter.submit(PresentmentTopology::new, PresentmentTopology.PROPERTIES_FILE, args);
    }

}
