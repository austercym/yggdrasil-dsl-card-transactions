package com.orwellg.yggdrasil.dsl.card.transactions.presentment.bolts;

import com.datastax.driver.core.Session;
import com.orwellg.umbrella.commons.repositories.scylla.LinkedAccountRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.LinkedAccountRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.LinkedAccount;
import com.orwellg.yggdrasil.card.transaction.commons.config.ScyllaSessionFactory;
import com.orwellg.yggdrasil.card.transaction.commons.model.TransactionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetLinkedAccount extends BasicRichBolt {

    private LinkedAccountRepository repository;
    private Logger LOG = LogManager.getLogger(GetLinkedAccount.class);
    private String propertyFile;

    public GetLinkedAccount(String propertyFile) {
        this.propertyFile = propertyFile;
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "retrieveValue"));
    }

    protected List<LinkedAccount> retrieve(TransactionInfo presentment) {
        long cardTransactionId = presentment.getDebitCardId();
        List<LinkedAccount> linkedAccount = repository.getLinkedAccountByDate(
                cardTransactionId,
                presentment.getTransactionDateTime().toInstant(ZoneOffset.UTC));
        return linkedAccount;
    }

    @Override
    public void execute(Tuple input) {
        try {

            String key = input.getStringByField("key");
            String processId = input.getStringByField("processId");

            LOG.debug("Key: {} | ProcessId: {} | Retrieving Linked Account from db", key, processId);

            Map<String, Object> values = new HashMap<>();
            values.put("key", key);
            values.put("processId", processId);
            values.put("eventData", input.getValueByField("eventData"));
            values.put("retrieveValue", retrieve((TransactionInfo) input.getValueByField("eventData")));
            send(input, values);

        } catch (Exception e) {
            LOG.error("Error retrieving linked account history. Message: {}", input, e.getMessage(), e);
            error(e, input);
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        initializeCardRepositories();
    }

    private void initializeCardRepositories() {
        Session session = ScyllaSessionFactory.getSession(propertyFile);
        repository = new LinkedAccountRepositoryImpl(session);
    }
}
