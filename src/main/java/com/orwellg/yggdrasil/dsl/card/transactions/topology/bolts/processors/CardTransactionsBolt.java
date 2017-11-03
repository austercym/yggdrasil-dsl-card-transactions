package com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.processors;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.repositories.CardTransactionRepository;
import com.orwellg.umbrella.commons.repositories.scylla.CardTransactionRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.generics.scylla.ScyllaRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;


import java.util.List;
import java.util.Map;

public class CardTransactionsBolt extends ScyllaRichBolt<List<CardTransaction>, Message> {

    private CardTransactionRepository repository;

    @Override
    protected void setScyllaConnectionParameters() {
        setScyllaNodes(ComponentFactory.getConfigurationParams().getScyllaConfig().getScyllaParams().getNodeList());
        setScyllaKeyspace(ComponentFactory.getConfigurationParams().getScyllaConfig().getScyllaParams().getKeyspace());
    }

    @Override
    protected List<CardTransaction> retrieve(Message data) {
        return repository.getCardTransaction(data.getTransLink());
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        repository = new CardTransactionRepositoryImpl(getScyllaNodes(), getScyllaKeyspace());
    }

}
