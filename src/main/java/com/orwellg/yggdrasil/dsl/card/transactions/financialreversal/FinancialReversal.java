package com.orwellg.yggdrasil.dsl.card.transactions.financialreversal;

import com.orwellg.yggdrasil.card.transaction.commons.utils.TopologySubmitter;

public class FinancialReversal {

    public static void main(String[] args) throws Exception {
        TopologySubmitter.submit(FinancialReversalTopology::new, FinancialReversalTopology.PROPERTIES_FILE, args);
    }
}
