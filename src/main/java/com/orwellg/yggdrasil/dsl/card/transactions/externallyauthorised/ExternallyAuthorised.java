package com.orwellg.yggdrasil.dsl.card.transactions.externallyauthorised;

import com.orwellg.yggdrasil.card.transaction.commons.utils.TopologySubmitter;

public class ExternallyAuthorised {

    public static void main(String[] args) throws Exception {
        TopologySubmitter.submit(ExternallyAuthorisedTopology::new, ExternallyAuthorisedTopology.PROPERTIES_FILE, args);
    }
}
