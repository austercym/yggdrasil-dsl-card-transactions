package com.orwellg.yggdrasil.dsl.card.transactions.savetoscylla;

import com.orwellg.yggdrasil.card.transaction.commons.utils.TopologySubmitter;

public class CardSaveScyllaMessageProcessed {

    public static void main(String[] args) throws Exception {
        TopologySubmitter.submit(CardSaveScyllaMessageProcessedTopology::new, CardSaveScyllaMessageProcessedTopology.PROPERTIES_FILE, args);
    }
}
