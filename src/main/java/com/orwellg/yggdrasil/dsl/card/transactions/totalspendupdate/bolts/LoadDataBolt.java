package com.orwellg.yggdrasil.dsl.card.transactions.totalspendupdate.bolts;

import com.datastax.driver.core.Session;
import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.avro.types.cards.MessageType;
import com.orwellg.umbrella.avro.types.cards.SpendGroup;
import com.orwellg.umbrella.commons.config.ScyllaConfig;
import com.orwellg.umbrella.commons.repositories.scylla.CardTransactionRepository;
import com.orwellg.umbrella.commons.repositories.scylla.SpendingTotalAmountsRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.SpendingTotalAmountsRepositoryImpl;
import com.orwellg.umbrella.commons.repositories.scylla.impl.cards.CardTransactionRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.JoinFutureBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.yggdrasil.card.transaction.commons.config.ScyllaSessionFactory;
import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfig;
import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfigFactory;
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

import static com.orwellg.yggdrasil.card.transaction.commons.utils.MessageTypeUtil.isAuthorisation;

public class LoadDataBolt extends JoinFutureBolt<MessageProcessed> {

    private static final long serialVersionUID = 1L;

    private Logger LOG = LogManager.getLogger(LoadDataBolt.class);

    private SpendingTotalAmountsRepository amountsRepository;

    private CardTransactionRepository cardTransactionRepository;
    private String propertyFile;


    public LoadDataBolt(String joinId, String propertyFile) {
        super(joinId);
        this.propertyFile = propertyFile;
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

        String zookeeperHost = (String) stormConf.get(Constants.ZK_HOST_LIST_PROPERTY);
        TopologyConfig topologyConfig = TopologyConfigFactory.getTopologyConfig(propertyFile, zookeeperHost);
        initializeCardRepositories(topologyConfig);
    }

    private void initializeCardRepositories(TopologyConfig topologyConfig) {
        Session session = ScyllaSessionFactory.getCardsSession(topologyConfig.getScyllaConfig());
        amountsRepository = new SpendingTotalAmountsRepositoryImpl(session);
        cardTransactionRepository = new CardTransactionRepositoryImpl(session);
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA, Fields.TOTAL_AMOUNTS, Fields.AUTHORISATION));
    }

    @Override
    protected void join(Tuple input, String key, String processId, MessageProcessed eventData) {

        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        LOG.info("{}Starting processing the total spend data load for key {}", logPrefix, key);

        try {
            CompletableFuture<SpendingTotalAmounts> totalAmountsFuture = retrieveTotalAmounts(
                    eventData.getDebitCardId(), eventData.getSpendGroup(), logPrefix);
            CompletableFuture<CardTransaction> cardTransactionFuture =
                    isPresentment(eventData)
                            ? retrieveCardTransaction(eventData.getTransactionId(), logPrefix)
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

    private boolean isPresentment(MessageProcessed eventData) {
        return MessageType.PRESENTMENT.equals(eventData.getMessageType());
    }

    private CompletableFuture<CardTransaction> retrieveCardTransaction(String transactionId, String logPrefix) {
        return CompletableFuture.supplyAsync(
                () -> {
                    LOG.info("{}Retrieving card transactions for TransactionId={} ...", logPrefix, transactionId);
                    List<CardTransaction> cardTransactions = cardTransactionRepository.getCardTransaction(transactionId);
                    LOG.info("{}Card transactions retrieved for TransactionId={}: {}", logPrefix, transactionId, cardTransactions);
                    if (cardTransactions == null || cardTransactions.isEmpty()) {
                        return null;
                    }
                    return cardTransactions.stream()
                            .filter(i -> isAuthorisation(i.getMessageType()))
                            .findFirst()
                            .orElse(null);
                });
    }

    private CompletableFuture<SpendingTotalAmounts> retrieveTotalAmounts(String cardId, SpendGroup totalType, String logPrefix) {
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
