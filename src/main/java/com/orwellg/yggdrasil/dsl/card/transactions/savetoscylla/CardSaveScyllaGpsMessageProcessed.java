package com.orwellg.yggdrasil.dsl.card.transactions.savetoscylla;

import com.orwellg.yggdrasil.dsl.card.transactions.common.TopologyLoader;

public class CardSaveScyllaGpsMessageProcessed {

    public static void main(String[] args) throws Exception {
        TopologyLoader.submitTopology(CardSaveScyllaGpsMessageProcessedTopology::new, CardSaveScyllaGpsMessageProcessedTopology.PROPERTIES_FILE, args);
    }
}
