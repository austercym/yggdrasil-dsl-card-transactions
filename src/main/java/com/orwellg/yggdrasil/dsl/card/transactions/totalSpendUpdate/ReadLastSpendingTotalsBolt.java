package com.orwellg.yggdrasil.dsl.card.transactions.totalSpendUpdate;

import com.orwellg.umbrella.avro.types.cards.SpendGroup;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.repositories.scylla.SpendingTotalAmountsRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.SpendingTotalAmountsRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.generics.scylla.ScyllaRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;

public class ReadLastSpendingTotalsBolt extends ScyllaRichBolt<SpendingTotalAmounts, GpsMessageProcessed> {

    private SpendingTotalAmountsRepository repository;

    @Override
    protected void setScyllaConnectionParameters() {
        ScyllaParams scyllaParams = ComponentFactory.getConfigurationParams().getCardsScyllaParams();
        repository = new SpendingTotalAmountsRepositoryImpl(scyllaParams.getNodeList(), scyllaParams.getKeyspace());
    }

    @Override
    protected SpendingTotalAmounts retrieve(GpsMessageProcessed data) {
        SpendGroup spendGroup = data.getSpendGroup();

        if (spendGroup == null)
            throw new IllegalArgumentException("Spend group is not set on GpsMessageProcessed");

        return repository.getTotalAmounts(data.getDebitCardId(), spendGroup);
    }
}
