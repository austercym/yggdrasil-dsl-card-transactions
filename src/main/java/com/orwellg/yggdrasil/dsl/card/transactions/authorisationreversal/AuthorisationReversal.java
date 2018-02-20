package com.orwellg.yggdrasil.dsl.card.transactions.authorisationreversal;

import com.orwellg.yggdrasil.dsl.card.transactions.common.TopologySubmitter;

public class AuthorisationReversal {

    public static void main(String[] args) throws Exception {
        TopologySubmitter.submit(AuthorisationReversalTopology::new, AuthorisationReversalTopology.PROPERTIES_FILE, args);
    }
}
