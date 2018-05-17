package com.orwellg.yggdrasil.dsl.card.transactions.totalspendupdate;

import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.yggdrasil.dsl.card.transactions.common.TopologySubmitter;
import com.orwellg.yggdrasil.dsl.card.transactions.savetoscylla.CardSaveScyllaMessageProcessedTopology;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;

public class TotalSpendUpdate {

    public static void main(String[] args) throws Exception {
        TopologySubmitter.submit(TotalSpendUpdateTopology::new, TotalSpendUpdateTopology.PROPERTIES_FILE, args);
    }
}
