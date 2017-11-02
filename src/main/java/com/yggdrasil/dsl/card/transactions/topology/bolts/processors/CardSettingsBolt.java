package com.yggdrasil.dsl.card.transactions.topology.bolts.processors;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.repositories.CardSettingsRepository;
import com.orwellg.umbrella.commons.repositories.scylla.CardSettingsRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.generics.scylla.ScyllaRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import java.util.Map;

public class CardSettingsBolt extends ScyllaRichBolt<CardSettings, Message> {

    private static final long serialVersionUID = 1L;

    private CardSettingsRepository cardSettingsRepository;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        cardSettingsRepository = new CardSettingsRepositoryImpl(getScyllaNodes(), getScyllaKeyspace());
    }

    @Override
    protected void setScyllaConnectionParameters() {
        setScyllaNodes(ComponentFactory.getConfigurationParams().getScyllaConfig().getScyllaParams().getNodeList());
        setScyllaKeyspace(ComponentFactory.getConfigurationParams().getScyllaConfig().getScyllaParams().getKeyspace());
    }

    @Override
    protected CardSettings retrieve(Message data) {
        CardSettings cardSettings = cardSettingsRepository.getCardSettings(Long.parseLong(data.getCustRef()));
        return cardSettings;
    }
}
