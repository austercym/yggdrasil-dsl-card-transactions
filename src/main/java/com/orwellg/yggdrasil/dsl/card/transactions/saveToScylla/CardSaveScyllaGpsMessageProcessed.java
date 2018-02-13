package com.orwellg.yggdrasil.dsl.card.transactions.savetoscylla;

import com.orwellg.yggdrasil.dsl.card.transactions.common.TopologySubmitter;

public class CardSaveScyllaGpsMessageProcessed {

    public static void main(String[] args) throws Exception {
        TopologySubmitter.submit(CardSaveScyllaGpsMessageProcessedTopology::new, CardSaveScyllaGpsMessageProcessedTopology.PROPERTIES_FILE, args);
    }
}
