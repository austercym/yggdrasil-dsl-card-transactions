package com.orwellg.yggdrasil.dsl.card.transactions.common;

import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfig;
import com.orwellg.umbrella.commons.storm.config.topology.TopologyConfigFactory;
import com.orwellg.umbrella.commons.storm.topology.component.base.AbstractTopology;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;

import java.util.function.Supplier;

public final class TopologySubmitter {

    private static final Logger LOG = LogManager.getLogger(TopologySubmitter.class);

    public static <TTopology extends AbstractTopology> void submit(Supplier<TTopology> supplier, String propertiesFile, String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        boolean local = false;

        if (args.length >= 1 && args[0].equals("local")) {
            LOG.info("*********** Local parameter received, will work with LocalCluster ************");
            local = true;
        }

        TTopology topology = supplier.get();

        if (local) {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(topology.name(), config(propertiesFile), topology.load());
            Thread.sleep(6000000L);
            localCluster.shutdown();
        } else {
            StormSubmitter.submitTopology(topology.name(), config(propertiesFile), topology.load());
        }
    }

    private static Config config(String propertiesFile) {
        TopologyConfig config = TopologyConfigFactory.getTopologyConfig(propertiesFile);
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxTaskParallelism(config.getTopologyMaxTaskParallelism());
        conf.setNumWorkers(config.getTopologyNumWorkers());
        return conf;
    }
}
