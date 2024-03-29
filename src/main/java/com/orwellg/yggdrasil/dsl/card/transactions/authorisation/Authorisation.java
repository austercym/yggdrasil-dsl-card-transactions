package com.orwellg.yggdrasil.dsl.card.transactions.authorisation;

import com.orwellg.yggdrasil.card.transaction.commons.utils.TopologySubmitter;

public class Authorisation {

    public static void main(String[] args) throws Exception {
        TopologySubmitter.submit(AuthorisationTopology::new, AuthorisationTopology.PROPERTIES_FILE, args);
    }
}
