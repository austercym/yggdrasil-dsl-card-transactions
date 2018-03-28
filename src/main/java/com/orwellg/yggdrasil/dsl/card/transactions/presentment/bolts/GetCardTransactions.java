package com.orwellg.yggdrasil.dsl.card.transactions.presentment.bolts;

import com.orwellg.umbrella.commons.repositories.scylla.CardTransactionRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.CardTransactionRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.generics.scylla.ScyllaRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.card.transaction.commons.model.TransactionInfo;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetCardTransactions extends ScyllaRichBolt<List<CardTransaction>, TransactionInfo> {

    private CardTransactionRepository repository;
    private static final Logger LOG = LogManager.getLogger(GetCardTransactions.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "retrieveValue"));
    }


    @Override
    protected void setScyllaConnectionParameters() {
        setScyllaNodes(ComponentFactory.getConfigurationParams().getCardsScyllaParams().getNodeList());
        setScyllaKeyspace(ComponentFactory.getConfigurationParams().getCardsScyllaParams().getKeyspace());
    }

    @Override
    protected List<CardTransaction> retrieve(TransactionInfo data) {
        return repository.getCardTransaction(data.getProviderTransactionId());
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        repository = new CardTransactionRepositoryImpl(getScyllaNodes(), getScyllaKeyspace());
    }

    @Override
    public void execute(Tuple input) {
        try {
            String key = input.getStringByField("key");
            String processId = input.getStringByField("processId");
            LOG.info("Key: {} | ProcessId: {} | Retrieving Card Transactions from db", key, processId);

            Map<String, Object> values = new HashMap<>();
            values.put("key", input.getStringByField("key"));
            values.put("processId", input.getStringByField("processId"));
            values.put("eventData", input.getValueByField("eventData"));
            values.put("retrieveValue", retrieve((TransactionInfo) input.getValueByField("eventData")));
            send(input, values);

        } catch (Exception e) {
            LOG.error("Error retrieving Card Transactions from db. Tuple: {}, Message: {}", input, e.getMessage(), e);
            error(e, input);
        }
    }

}
