package com.orwellg.yggdrasil.dsl.card.transactions.presentment;

import com.orwellg.yggdrasil.dsl.card.transactions.common.TopologySubmitter;

public class CardPresentment {

    public static void main(String[] args) throws Exception {
        TopologySubmitter.submit(CardPresentmentTopology::new, CardPresentmentTopology.PROPERTIES_FILE, args);
    }

}
