package com.orwellg.yggdrasil.dsl.card.transactions.authorisation.bolts;

import com.orwellg.umbrella.avro.types.cards.SpendGroup;
import com.orwellg.umbrella.commons.repositories.scylla.AccountBalanceRepository;
import com.orwellg.umbrella.commons.repositories.scylla.CardSettingsRepository;
import com.orwellg.umbrella.commons.repositories.scylla.SpendingTotalAmountsRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.AccountBalanceRepositoryImpl;
import com.orwellg.umbrella.commons.repositories.scylla.impl.CardSettingsRepositoryImpl;
import com.orwellg.umbrella.commons.repositories.scylla.impl.SpendingTotalAmountsRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.JoinFutureBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.AccountBalance;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.yggdrasil.dsl.card.transactions.config.ScyllaParams;
import com.orwellg.yggdrasil.dsl.card.transaction.commons.model.TransactionInfo;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class LoadDataBolt extends JoinFutureBolt<TransactionInfo> {

    private static final long serialVersionUID = 1L;

    private Logger LOG = LogManager.getLogger(LoadDataBolt.class);

    private CardSettingsRepository cardSettingsRepository;

    private SpendingTotalAmountsRepository totalAmountsRepository;

    private AccountBalanceRepository accountBalanceRepository;

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
        cardSettingsRepository = new CardSettingsRepositoryImpl(nodeList, keyspace);
        totalAmountsRepository = new SpendingTotalAmountsRepositoryImpl(nodeList, keyspace);
        accountBalanceRepository = new AccountBalanceRepositoryImpl(nodeList, keyspace);
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA,
                Fields.CARD_SETTINGS, Fields.ACCOUNT_BALANCE, Fields.SPENDING_TOTALS));
    }

    @Override
    protected void join(Tuple input, String key, String processId, TransactionInfo eventData) {

        long startTime = System.currentTimeMillis();
        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        LOG.info("{}Starting processing the join data load for key {}", logPrefix, key);

        try {
            long cardId = eventData.getDebitCardId();
            SpendGroup totalType = eventData.getSpendGroup();

            CompletableFuture<CardSettings> settingsFuture = retrieveCardSettings(cardId, logPrefix);
            CompletableFuture<AccountBalance> accountTransactionLogFuture =
                    retrieveAccountBalance(settingsFuture, logPrefix);
            CompletableFuture<SpendingTotalAmounts> totalFuture =
                    eventData.getIsBalanceEnquiry()
                            ? CompletableFuture.completedFuture(null)
                            : retrieveTotalAmounts(cardId, totalType, new Date(), logPrefix);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_DATA, eventData);
            values.put(Fields.CARD_SETTINGS, settingsFuture.get());
            values.put(Fields.ACCOUNT_BALANCE, accountTransactionLogFuture.get());
            values.put(Fields.SPENDING_TOTALS, totalFuture.get());

            send(input, values);

            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            LOG.info("{}Processed the join data load for key {}. (Execution time: {} ms)", logPrefix, key, elapsedTime);
        } catch (Exception e) {
            LOG.error("{}Error processing the authorisation data load. Message: {}", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    private CompletableFuture<CardSettings> retrieveCardSettings(Long cardId, String logPrefix) {
        return CompletableFuture.supplyAsync(
                () -> {
                    long startTime = System.currentTimeMillis();
                    LOG.info("{}Retrieving card settings for debitCardId={} ...", logPrefix, cardId);
                    CardSettings cardSettings = cardSettingsRepository.getCardSettings(cardId);
                    long stopTime = System.currentTimeMillis();
                    long elapsedTime = stopTime - startTime;
                    LOG.info("{}Card settings retrieved for debitCardId={}: {}. (Execution time: {} ms)", logPrefix, cardId, cardSettings, elapsedTime);
                    return cardSettings;
                });
    }

    private CompletableFuture<AccountBalance> retrieveAccountBalance(CompletableFuture<CardSettings> settingsFuture, String logPrefix) {
        return settingsFuture.thenApply(settings -> {
            long startTime = System.currentTimeMillis();
            if (settings == null){
                LOG.info("{}No card settings - cannot retrieve linked account balance", logPrefix);
                return null;
            }
            String linkedAccountId = settings.getLinkedAccountId();
            LOG.info("{}Retrieving account balance for account id {} ...", logPrefix, linkedAccountId);
            AccountBalance transactionLog = accountBalanceRepository.getLastByAccountId(linkedAccountId);
            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            LOG.info("{}Account balance retrieved for account id {}: {}. (Execution time: {} ms)", logPrefix, linkedAccountId, transactionLog, elapsedTime);
            return transactionLog;
        });
    }

    private CompletableFuture<SpendingTotalAmounts> retrieveTotalAmounts(long cardId, SpendGroup totalType, Date date, String logPrefix) {
        return CompletableFuture.supplyAsync(
                () -> {
                    long startTime = System.currentTimeMillis();
                    LOG.info(
                            "{}Retrieving total transaction amounts for debitCardId={}, totalType={} ...",
                            logPrefix, cardId, totalType);
                    SpendingTotalAmounts totalAmounts = totalAmountsRepository.getTotalAmounts(cardId, totalType);
                    long stopTime = System.currentTimeMillis();
                    long elapsedTime = stopTime - startTime;
                    LOG.info("{}Total transaction amounts retrieved for debitCardId={}, totalType={}: {}. (Execution time: {} ms)",
                            logPrefix, cardId, totalType, totalAmounts, elapsedTime);
                    return totalAmounts;
                });
    }
}
