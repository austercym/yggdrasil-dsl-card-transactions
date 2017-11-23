package com.orwellg.yggdrasil.dsl.card.transactions.config;

import com.netflix.config.DynamicPropertyFactory;
import com.orwellg.umbrella.commons.beans.config.zookeeper.ZkConfigurationParams;
import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.utils.config.ZookeeperUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CardsScyllaConfig extends ZkConfigurationParams {

    private final static Logger LOG = LogManager.getLogger(CardsScyllaConfig.class);


    public final static String DEFAULT_PROPERTIES_FILE = "yggdrasil-dsl-card-transactions-scylla.properties";

    public static final String DEFAULT_SUB_BRANCH      = "/yggdrasil/card/transactions/dsl/scylla";

    public static final String ZK_SUB_BRANCH_KEY        = "zookeeper.scylla.cards.config.subbranch";


    private ScyllaParams scyllaParams;

    public ScyllaParams getScyllaParams() { return scyllaParams; }

    @Override
    protected void loadParameters() {

        DynamicPropertyFactory dynamicPropertyFactory = null;
        try {
            dynamicPropertyFactory = ZookeeperUtils.getDynamicPropertyFactory();
        } catch (Exception e) {
            LOG.error("Error when try get the dynamic property factory from Zookeeper. Message: {}",  e.getMessage(), e);
        }

        if (dynamicPropertyFactory != null) {
            LOG.info("Loading cards scylla parameters....");
            scyllaParams  = new ScyllaParams(
                    dynamicPropertyFactory.getStringProperty(Constants.SCYLLA_NODE_LIST, ScyllaParams.DEFAULT_SCYLA_NODE_LIST),
                    dynamicPropertyFactory.getStringProperty(Constants.SCYLLA_NODE_HOST_LIST, ScyllaParams.DEFAULT_SCYLA_NODE_HOST_LIST),
                    dynamicPropertyFactory.getStringProperty(Constants.SCYLLA_KEYSPACE, ScyllaParams.DEFAULT_SCYLA_KEYSPACE)
            );
        }
    }

    public CardsScyllaConfig(String propertiesFileName) {
        LOG.info("Getting properties of {} : [{}, {}]" , propertiesFileName, ZK_SUB_BRANCH_KEY, DEFAULT_SUB_BRANCH);
        super.setPropertiesFile(propertiesFileName);
        super.setApplicationRootConfig(ZK_SUB_BRANCH_KEY, DEFAULT_SUB_BRANCH);
    }

    public CardsScyllaConfig() {
        this(DEFAULT_PROPERTIES_FILE);
    }
}
