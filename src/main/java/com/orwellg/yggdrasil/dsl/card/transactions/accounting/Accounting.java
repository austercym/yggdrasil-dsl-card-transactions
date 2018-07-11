package com.orwellg.yggdrasil.dsl.card.transactions.accounting;

import com.orwellg.yggdrasil.card.transaction.commons.utils.TopologySubmitter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Accounting {

    private static final Logger LOG = LogManager.getLogger(Accounting.class);

    public static void main(String[] args) throws Exception {
        TopologySubmitter.submit(AccountingTopology::new, AccountingTopology.PROPERTIES_FILE, args);
    }
}
