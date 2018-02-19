package com.orwellg.yggdrasil.dsl.card.transactions.config;

import com.orwellg.umbrella.commons.beans.config.kafka.SubscriberKafkaConfiguration;
import com.orwellg.yggdrasil.commons.config.topology.DSLTopologyConfig;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TopologyConfig extends DSLTopologyConfig {


    public TopologyConfig() {
        super(DEFAULT_PROPERTIES_FILE);
    }

    public TopologyConfig(String propertiesFile) {
        super(propertiesFile);
    }

    public List<SubscriberKafkaConfiguration> getKafkaSubscriberSpoutConfigs() {

        List<SubscriberKafkaConfiguration> configs = new ArrayList<>();

        String topicsProperty = propertiesUtils.getStringProperty("subscriber.topics");
        if (topicsProperty != null && !topicsProperty.isEmpty()) {
            String[] topics = topicsProperty.split(",");

            for (String topic : topics) {

                SubscriberKafkaConfiguration subsConf = new SubscriberKafkaConfiguration();

                setSubsConfData(subsConf, true, topic);

                configs.add(subsConf);
            }
        }

        return configs;
    }

    protected void setSubsConfData(SubscriberKafkaConfiguration subsConf, boolean forceSet, String topic) {
        if (forceSet || subsConf.getZookeeper() == null) {
            subsConf.setZookeeper(new SubscriberKafkaConfiguration.Zookeeper());
            subsConf.getZookeeper().setHost(this.getZookeeperConnection());
        }

        if (forceSet || subsConf.getBootstrap() == null) {
            subsConf.setBootstrap(new SubscriberKafkaConfiguration.Bootstrap());
            subsConf.getBootstrap().setHost(this.getKafkaBootstrapHosts());
        }

        if (forceSet || subsConf.getTopic() == null) {
            // subscriber.topic property in topo.properties
            subsConf.setTopic(new SubscriberKafkaConfiguration.Topic());
            subsConf.getTopic().setName(Collections.singletonList(topic));
            // default value. Cannot be set for now in .properties or zookeeper. Can be
            // overriden with yaml
            subsConf.getTopic().setCommitInterval(3);
        }

        if (forceSet || subsConf.getApplication() == null) {
            // from topo.properties "application.id"
            subsConf.setApplication(new SubscriberKafkaConfiguration.Application());
            subsConf.getApplication().setId(propertiesUtils.getStringProperty("application.id"));
            // from topo.properties "application.id"
            subsConf.getApplication().setName(propertiesUtils.getStringProperty("application.id"));
        }

        if (forceSet || subsConf.getConfiguration() == null) {
            // default value. Cannot be set for now in .properties or zookeeper. Can be
            // overriden with yaml
            subsConf.setConfiguration(new SubscriberKafkaConfiguration.Configuration());
            subsConf.getConfiguration().setAutoOffsetReset("earliest");
        }
    }
}
