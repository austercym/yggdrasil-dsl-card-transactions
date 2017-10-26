package com.yggdrasil.dsl.card.transactions.topology.bolts.processors;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.repositories.LinkedAccountRepository;
import com.orwellg.umbrella.commons.repositories.scylla.LinkedAccountRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.generics.scylla.ScyllaRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.LinkedAccountHistory;
import com.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import java.util.List;
import java.util.Map;

public class LinkedAccountBolt extends ScyllaRichBolt<List<LinkedAccountHistory>, Message> {


    private LinkedAccountRepository repository;

    @Override
    protected void setScyllaConnectionParameters() {
        setScyllaNodes(ComponentFactory.getConfigurationParams().getScyllaConfig().getScyllaParams().getNodeList());
        setScyllaKeyspace(ComponentFactory.getConfigurationParams().getScyllaConfig().getScyllaParams().getKeyspace());
    }

    @Override
    protected List<LinkedAccountHistory> retrieve(Message data) {
        //todo:
        //List<LinkedAccountHistory> cardSettings = repository.getLinkedAccountByDate(data.getCustRef(), new Date());
        //return cardSettings;
        return null;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        repository = new LinkedAccountRepositoryImpl(getScyllaNodes(), getScyllaKeyspace());
    }
}
