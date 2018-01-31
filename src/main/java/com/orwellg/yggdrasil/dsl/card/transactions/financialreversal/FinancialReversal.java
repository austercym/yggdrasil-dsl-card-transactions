package com.orwellg.yggdrasil.dsl.card.transactions.financialreversal;

import com.orwellg.yggdrasil.dsl.card.transactions.common.TopologyLoader;

public class FinancialReversal {

    public static void main(String[] args) throws Exception {
        TopologyLoader.submitTopology(FinancialReversalTopology::new, FinancialReversalTopology.PROPERTIES_FILE, args);
    }
}
