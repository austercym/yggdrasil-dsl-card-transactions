package com.orwellg.yggdrasil.dsl.card.transactions.savetoscylla;

import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;

public class CardSaveScyllaGpsMessageProcessed {

    private static final Logger LOG = LogManager.getLogger(CardSaveScyllaGpsMessageProcessed.class);

    public static void main(String[] args) throws Exception {
        boolean local = false;

        if (args.length >= 1 && args[0].equals("local")) {
            LOG.info("*********** Local parameter received, will work with LocalCluster ************");
            local = true;
        }

        CardSaveScyllaGpsMessageProcessedTopology topology = new CardSaveScyllaGpsMessageProcessedTopology();

        if (local) {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(topology.name(), config(), topology.load());
            Thread.sleep(6000000L);
            localCluster.shutdown();
        } else {
            StormSubmitter.submitTopology(topology.name(), config(), topology.load());
        }
    }

    private static Config config() {
        TopologyConfig config = TopologyConfigFactory.getTopologyConfig(CardSaveScyllaGpsMessageProcessedTopology.PROPERTIES_FILE);
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(config.getTopologyMaxTaskParallelism());
        conf.setNumWorkers(config.getTopologyNumWorkers());
        return conf;
    }
}
