package com.orwellg.yggdrasil.dsl.card.transactions.secondpresentment;

import com.orwellg.yggdrasil.card.transaction.commons.utils.TopologySubmitter;

public class SecondPresentment {

    public static void main(String[] args) throws Exception {
        TopologySubmitter.submit(SecondPresentmentTopology::new, SecondPresentmentTopology.PROPERTIES_FILE, args);
    }
}
