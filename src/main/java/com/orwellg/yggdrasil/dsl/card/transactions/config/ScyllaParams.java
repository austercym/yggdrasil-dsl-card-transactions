package com.orwellg.yggdrasil.dsl.card.transactions.config;

import com.netflix.config.DynamicStringProperty;

public class ScyllaParams {

    public final static String DEFAULT_SCYLLA_NODE_LIST = "localhost:9042";

    public final static String DEFAULT_SCYLLA_NODE_HOST_LIST = "localhost";

    public final static String DEFAULT_SCYLLA_KEYSPACE = "cards";


    private DynamicStringProperty nodeList;

    private DynamicStringProperty nodesHostList;

    private DynamicStringProperty keyspace;

    public ScyllaParams(DynamicStringProperty nodes, DynamicStringProperty nodesHostList, DynamicStringProperty keyspace) {
        setKeyspace(keyspace);
        setNodeList(nodes);
        setNodeHostList(nodesHostList);
    }

    public String getNodeList() {
        return (nodeList != null) ? nodeList.get() : DEFAULT_SCYLLA_NODE_LIST;
    }

    private void setNodeList(DynamicStringProperty nodeList) {
        this.nodeList = nodeList;
    }

    public String getHostList() {
        return (nodesHostList != null) ? nodesHostList.get() : DEFAULT_SCYLLA_NODE_HOST_LIST;
    }

    private void setNodeHostList(DynamicStringProperty nodesHostList) {
        this.nodesHostList = nodesHostList;
    }

    public String getKeyspace() {
        return (keyspace != null) ? keyspace.get() : DEFAULT_SCYLLA_KEYSPACE;
    }

    private void setKeyspace(DynamicStringProperty keyspace) {
        this.keyspace = keyspace;
    }
}
