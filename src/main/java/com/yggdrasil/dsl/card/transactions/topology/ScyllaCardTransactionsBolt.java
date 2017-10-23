package com.yggdrasil.dsl.card.transactions.topology;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.repositories.CardTransactionRepository;
import com.orwellg.umbrella.commons.repositories.scylla.CardTransactionRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.generics.scylla.ScyllaRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import java.util.List;
import java.util.Map;

public class ScyllaCardTransactionsBolt extends ScyllaRichBolt<List<CardTransaction>, Message> {

    private CardTransactionRepository repository;

    @Override
    protected void setScyllaConnectionParameters() {
        setScyllaNodes("localhost:9042"); //todo: config
        setScyllaKeyspace("cards"); //todo: config
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
