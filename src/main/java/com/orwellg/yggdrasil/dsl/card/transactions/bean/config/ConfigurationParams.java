package com.orwellg.yggdrasil.dsl.card.transactions.bean.config;

import com.orwellg.umbrella.commons.beans.config.zookeeper.ZkConfigurationParams;
import com.orwellg.umbrella.commons.config.ScyllaConfig;
import com.orwellg.yggdrasil.commons.config.NetworkConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;

public class ConfigurationParams extends ZkConfigurationParams implements Serializable {

    public static final String DEFAULT_PROPERTIES_FILE = "yggdrasil-dsl-card-transactions.properties";
    public static final String DEFAULT_SUB_BRANCH = "/yggdrasil/card/transactions/dsl";
    public static final String DEFAULT_SCYLLA_SUB_BRANCH = DEFAULT_SUB_BRANCH + "/scylla";
    public static final String ZK_SUB_BRANCH_KEY = "zookeeper.dsl.card.transactions.config.subbranch";
    private final static Logger LOG = LogManager.getLogger(ConfigurationParams.class);
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    public NetworkConfig networkConfig;
    private ScyllaConfig scyllaConfig;

    public ConfigurationParams() {
        LOG.info("Loading configuration params.");
        scyllaConfig = new ScyllaConfig(DEFAULT_PROPERTIES_FILE);
        scyllaConfig.setApplicationRootConfig(ScyllaConfig.ZK_SUB_BRANCH_KEY, DEFAULT_SCYLLA_SUB_BRANCH);
        networkConfig = new NetworkConfig(DEFAULT_PROPERTIES_FILE);
        super.setPropertiesFile(DEFAULT_PROPERTIES_FILE);
        super.setApplicationRootConfig(ZK_SUB_BRANCH_KEY, DEFAULT_SUB_BRANCH);
        LOG.info("Configuration params loaded.");
    }

    public ScyllaConfig getScyllaConfig() {
        return scyllaConfig;
    }

    public void setScyllaConfig(ScyllaConfig scyllaConfig) {
        this.scyllaConfig = scyllaConfig;
    }

    public NetworkConfig getNetworkConfig() {
        return networkConfig;
    }

    @Override
    protected void loadParameters() {

    }

    @Override
    public void start() throws Exception {
        LOG.info("Starting configuration params.");
        super.start();
        scyllaConfig.start();
        LOG.info(
                "Scylla configuration: NodeList={}, HostList={}, Keyspace={}",
                scyllaConfig.getScyllaParams().getNodeList(),
                scyllaConfig.getScyllaParams().getHostList(),
                scyllaConfig.getScyllaParams().getKeyspace());
        networkConfig.start();
        LOG.info("Configuration params started.");
    }

    @Override
    public void close() {
        scyllaConfig.close();
        networkConfig.close();
        super.close();
    }
}
