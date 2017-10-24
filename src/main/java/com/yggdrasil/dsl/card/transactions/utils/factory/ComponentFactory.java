package com.yggdrasil.dsl.card.transactions.utils.factory;

import com.yggdrasil.dsl.card.transactions.bean.config.ConfigurationParams;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ComponentFactory {

    private final static Logger LOG = LogManager.getLogger(ComponentFactory.class);

    private static ConfigurationParams configurationParams;

    public synchronized static void initConfigurationParams() {

        if (configurationParams == null) {
            configurationParams = new ConfigurationParams();
            try {
                configurationParams.start();
            } catch (Exception e) {
                LOG.error("The configuration params cannot be started. The system work with the parameters for default. Message: {}",  e.getMessage(),  e);
            }
        }
    }

    public synchronized static ConfigurationParams getConfigurationParams() {
        initConfigurationParams();
        return configurationParams;
    }
}
