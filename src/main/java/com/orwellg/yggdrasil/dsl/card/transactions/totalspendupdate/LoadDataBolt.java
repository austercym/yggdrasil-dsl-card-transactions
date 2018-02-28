package com.orwellg.yggdrasil.dsl.card.transactions.totalspendupdate;

import com.orwellg.umbrella.avro.types.cards.CardMessageProcessed;
import com.orwellg.umbrella.avro.types.cards.SpendGroup;
import com.orwellg.umbrella.commons.repositories.scylla.CardTransactionRepository;
import com.orwellg.umbrella.commons.repositories.scylla.SpendingTotalAmountsRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.CardTransactionRepositoryImpl;
import com.orwellg.umbrella.commons.repositories.scylla.impl.SpendingTotalAmountsRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.JoinFutureBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.yggdrasil.dsl.card.transactions.config.ScyllaParams;
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
import java.util.concurrent.CompletableFuture;

public class LoadDataBolt extends JoinFutureBolt<CardMessageProcessed> {

    private static final long serialVersionUID = 1L;

    private Logger LOG = LogManager.getLogger(LoadDataBolt.class);

    private SpendingTotalAmountsRepository amountsRepository;

    private CardTransactionRepository cardTransactionRepository;


    public LoadDataBolt(String joinId) {
        super(joinId);
    }

    @Override
    public String getEventSuccessStream() {
        return KafkaSpout.EVENT_SUCCESS_STREAM;
    }

    @Override
    public String getEventErrorStream() {
        return KafkaSpout.EVENT_ERROR_STREAM;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        initializeCardRepositories();
    }

    private void initializeCardRepositories() {
        ScyllaParams scyllaParams = ComponentFactory.getConfigurationParams().getCardsScyllaParams();
        String nodeList = scyllaParams.getNodeList();
        String keyspace = scyllaParams.getKeyspace();
        amountsRepository = new SpendingTotalAmountsRepositoryImpl(nodeList, keyspace);
        cardTransactionRepository = new CardTransactionRepositoryImpl(nodeList, keyspace);
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA, Fields.TOTAL_AMOUNTS, Fields.AUTHORISATION));
    }

    @Override
    protected void join(Tuple input, String key, String processId, CardMessageProcessed eventData) {

        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        LOG.info("{}Starting processing the total spend data load for key {}", logPrefix, key);

        try {
            CompletableFuture<SpendingTotalAmounts> totalAmountsFuture = retrieveTotalAmounts(
                    eventData.getDebitCardId(), eventData.getSpendGroup(), logPrefix);
            CompletableFuture<CardTransaction> cardTransactionFuture = "P".equalsIgnoreCase(eventData.getGpsMessageType())
                    ? retrieveCardTransaction(eventData.getGpsTransactionLink(), logPrefix)
                    : CompletableFuture.completedFuture(null);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_DATA, eventData);
            values.put(Fields.TOTAL_AMOUNTS, totalAmountsFuture.get());
            values.put(Fields.AUTHORISATION, cardTransactionFuture.get());

            send(input, values);

        } catch (Exception e) {
            LOG.error("{}Error processing the total spend update data load. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    private CompletableFuture<CardTransaction> retrieveCardTransaction(String gpsTransactionLink, String logPrefix) {
        return CompletableFuture.supplyAsync(
                () -> {
                    LOG.info("{}Retrieving card transactions for GpsTransactionLink={} ...", logPrefix, gpsTransactionLink);
                    List<CardTransaction> cardTransactions = cardTransactionRepository.getCardTransaction(gpsTransactionLink);
                    LOG.info("{}Card transactions retrieved for GpsTransactionLink={}: {}", logPrefix, gpsTransactionLink, cardTransactions);
                    if (cardTransactions == null || cardTransactions.isEmpty()) {
                        return null;
                    }
                    return cardTransactions.stream()
                            .filter(i -> "A".equalsIgnoreCase(i.getGpsMessageType()))
                            .findFirst()
                            .get();
                });
    }

    private CompletableFuture<SpendingTotalAmounts> retrieveTotalAmounts(long cardId, SpendGroup totalType, String logPrefix) {
        return CompletableFuture.supplyAsync(
                () -> {
                    LOG.info(
                            "{}Retrieving total transaction amounts for debitCardId={}, totalType={} ...",
                            logPrefix, cardId, totalType);
                    SpendingTotalAmounts totalAmounts = amountsRepository.getTotalAmounts(cardId, totalType);
                    LOG.info("{}Total transaction amounts retrieved for debitCardId={}, totalType={}: {}",
                            logPrefix, cardId, totalType, totalAmounts);
                    return totalAmounts;
                });
    }
}
