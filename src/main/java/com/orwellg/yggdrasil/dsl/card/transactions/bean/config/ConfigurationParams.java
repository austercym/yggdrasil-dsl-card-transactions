package com.orwellg.yggdrasil.dsl.card.transactions.bean.config;

import com.netflix.config.DynamicPropertyFactory;
import com.orwellg.umbrella.commons.beans.config.zookeeper.ZkConfigurationParams;
import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.utils.config.ZookeeperUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.yggdrasil.commons.config.NetworkConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;

public class ConfigurationParams extends ZkConfigurationParams implements Serializable {

    private static final String DEFAULT_PROPERTIES_FILE = "yggdrasil-dsl-card-transactions.properties";
    private static final String DEFAULT_SUB_BRANCH = "/yggdrasil/card/transactions/dsl";
    private static final String ZK_SUB_BRANCH_KEY = "zookeeper.dsl.card.transactions.config.subbranch";

    private static final String CARDS_SCYLLA_NODE_LIST = "yggdrasil.cards.scylla.node.list";
    private static final String CARDS_SCYLLA_NODE_HOST_LIST = "yggdrasil.cards.scylla.node.host.list";
    private static final String CARDS_SCYLLA_KEYSPACE  = "yggdrasil.cards.scylla.keyspace";

    private final static Logger LOG = LogManager.getLogger(ConfigurationParams.class);

    private static final long serialVersionUID = 1L;

    private NetworkConfig networkConfig;
    private ScyllaParams cardsScyllaParams;
    private ScyllaParams transactionLogScyllaParams;

    public ConfigurationParams() {
        LOG.info("Loading configuration params.");

        networkConfig = new NetworkConfig(DEFAULT_PROPERTIES_FILE);

        super.setPropertiesFile(DEFAULT_PROPERTIES_FILE);
        super.setApplicationRootConfig(ZK_SUB_BRANCH_KEY, DEFAULT_SUB_BRANCH);

        LOG.info("Configuration params loaded.");
    }

    public NetworkConfig getNetworkConfig() {
        return networkConfig;
    }

    @Override
    protected void loadParameters() {

        DynamicPropertyFactory dynamicPropertyFactory = null;
        try {
            dynamicPropertyFactory = ZookeeperUtils.getDynamicPropertyFactory();
        } catch (Exception e) {
            LOG.error("Error when try get the dynamic property factory from Zookeeper. Message: {}",  e.getMessage(), e);
        }

        if (dynamicPropertyFactory != null) {
            LOG.info("Loading scylla parameters....");
            cardsScyllaParams = new ScyllaParams(
                    dynamicPropertyFactory.getStringProperty(CARDS_SCYLLA_NODE_LIST, ScyllaParams.DEFAULT_SCYLA_NODE_LIST),
                    dynamicPropertyFactory.getStringProperty(CARDS_SCYLLA_NODE_HOST_LIST, ScyllaParams.DEFAULT_SCYLA_NODE_HOST_LIST),
                    dynamicPropertyFactory.getStringProperty(CARDS_SCYLLA_KEYSPACE, ScyllaParams.DEFAULT_SCYLA_KEYSPACE)
            );
            transactionLogScyllaParams = new ScyllaParams(
                    dynamicPropertyFactory.getStringProperty(Constants.SCYLLA_NODE_LIST, ScyllaParams.DEFAULT_SCYLA_NODE_LIST),
                    dynamicPropertyFactory.getStringProperty(Constants.SCYLLA_NODE_HOST_LIST, ScyllaParams.DEFAULT_SCYLA_NODE_HOST_LIST),
                    dynamicPropertyFactory.getStringProperty(Constants.SCYLLA_KEYSPACE, ScyllaParams.DEFAULT_SCYLA_KEYSPACE)
            );
        }
    }

    @Override
    public void start() throws Exception {
        LOG.info("Starting configuration params.");
        super.start();

        LOG.info(
                "Card's Scylla configuration: NodeList={}, HostList={}, Keyspace={}",
                getCardsScyllaParams().getNodeList(),
                getCardsScyllaParams().getHostList(),
                getCardsScyllaParams().getKeyspace());

        LOG.info(
                "TransactionLog's Scylla configuration: NodeList={}, HostList={}, Keyspace={}",
                getTransactionLogScyllaParams().getNodeList(),
                getTransactionLogScyllaParams().getHostList(),
                getTransactionLogScyllaParams().getKeyspace());

        networkConfig.start();
        LOG.info("Configuration params started.");
    }

    @Override
    public void close() {
        networkConfig.close();
        super.close();
    }

    public ScyllaParams getCardsScyllaParams() {
        return cardsScyllaParams;
    }

    public ScyllaParams getTransactionLogScyllaParams() {
        return transactionLogScyllaParams;
    }
}
