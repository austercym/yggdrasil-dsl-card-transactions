package com.orwellg.yggdrasil.dsl.card.transactions.earmarking;

import com.orwellg.yggdrasil.card.transaction.commons.utils.TopologySubmitter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Earmarking {

    private static final Logger LOG = LogManager.getLogger(Earmarking.class);

    public static void main(String[] args) throws Exception {
        TopologySubmitter.submit(EarmarkingTopology::new, EarmarkingTopology.PROPERTIES_FILE, args);
    }
}
