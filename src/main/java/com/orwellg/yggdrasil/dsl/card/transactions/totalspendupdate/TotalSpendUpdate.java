package com.orwellg.yggdrasil.dsl.card.transactions.totalspendupdate;

import com.orwellg.yggdrasil.card.transaction.commons.utils.TopologySubmitter;

public class TotalSpendUpdate {

    public static void main(String[] args) throws Exception {
        TopologySubmitter.submit(TotalSpendUpdateTopology::new, TotalSpendUpdateTopology.PROPERTIES_FILE, args);
    }
}
