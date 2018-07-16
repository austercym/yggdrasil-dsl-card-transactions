package com.orwellg.yggdrasil.dsl.card.transactions.presentment.bolts;

import com.datastax.driver.core.Session;
import com.orwellg.umbrella.commons.repositories.scylla.CardTransactionRepository;
import com.orwellg.umbrella.commons.repositories.scylla.cards.TransactionMatchingRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.cards.CardTransactionRepositoryImpl;
import com.orwellg.umbrella.commons.repositories.scylla.impl.cards.TransactionMatchingRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.generics.scylla.ScyllaRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.card.transaction.commons.config.ScyllaSessionFactory;
import com.orwellg.yggdrasil.card.transaction.commons.model.TransactionInfo;
import com.orwellg.yggdrasil.card.transaction.commons.transactionmatching.TransactionMatcher;
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
    private TransactionMatchingRepository matchingRepository;
    private TransactionMatcher transactionMatcher;
    private static final Logger LOG = LogManager.getLogger(GetCardTransactions.class);
    private String propertyFile;

    public GetCardTransactions(String propertyFile) {
        this.propertyFile = propertyFile;
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "retrieveValue"));
    }

    @Override
    protected void setScyllaConnectionParameters() {
    }

    @Override
    protected List<CardTransaction> retrieve(TransactionInfo data) {
        return repository.getCardTransaction(data.getTransactionId());
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        Session session = ScyllaSessionFactory.getSession(propertyFile);
        repository = new CardTransactionRepositoryImpl(session);
        matchingRepository = new TransactionMatchingRepositoryImpl(session);
        transactionMatcher = new TransactionMatcher(matchingRepository);
    }

    @Override
    public void execute(Tuple input) {
        String key = input.getStringByField("key");
        String processId = input.getStringByField("processId");
        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);
        try {
            TransactionInfo eventData = (TransactionInfo) input.getValueByField("eventData");

            LOG.info("{}Matching transaction for: {}", logPrefix, eventData);
            eventData.setTransactionId(transactionMatcher.tryGetMatchingTransactionId(eventData.getMessage()));

            List<CardTransaction> cardTransactions = null;
            if (eventData.getTransactionId() == null) {
                LOG.info("{}No transaction id matched - offline transaction", logPrefix);
                eventData.setTransactionId(processId);
            } else {
                LOG.info("{}Retrieving Card Transactions from db", logPrefix);
                cardTransactions = retrieve(eventData);
            }

            Map<String, Object> values = new HashMap<>();
            values.put("key", input.getStringByField("key"));
            values.put("processId", input.getStringByField("processId"));
            values.put("eventData", input.getValueByField("eventData"));
            values.put("retrieveValue", cardTransactions);
            send(input, values);

        } catch (Exception e) {
            LOG.error(
                    "{}Error retrieving Card Transactions from db. Tuple: {}, Message: {}",
                    logPrefix, input, e.getMessage(), e);
            error(e, input);
        }
    }
}
