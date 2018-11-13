package com.orwellg.yggdrasil.dsl.card.transactions.accounting.bolts;

import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfig;

public class TestableAccountingCommandBolt extends AccountingCommandBolt {
    @Override
    protected void initialiseProcessorCluster(TopologyConfig topologyConfig) {
        // nada
    }
}
