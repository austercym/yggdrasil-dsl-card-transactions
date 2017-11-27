package com.orwellg.yggdrasil.dsl.card.transactions.authorisation;

import com.orwellg.umbrella.avro.types.cards.SpendGroup;
import com.orwellg.umbrella.commons.repositories.scylla.AccountTransactionLogRepository;
import com.orwellg.umbrella.commons.repositories.scylla.CardSettingsRepository;
import com.orwellg.umbrella.commons.repositories.scylla.SpendingTotalAmountsRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.AccountTransactionLogRepositoryImpl;
import com.orwellg.umbrella.commons.repositories.scylla.impl.CardSettingsRepositoryImpl;
import com.orwellg.umbrella.commons.repositories.scylla.impl.SpendingTotalAmountsRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.JoinFutureBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.types.scylla.entities.accounting.AccountTransactionLog;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.yggdrasil.dsl.card.transactions.config.ScyllaParams;
import com.orwellg.yggdrasil.dsl.card.transactions.model.AuthorisationMessage;
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

public class LoadDataBolt extends JoinFutureBolt<AuthorisationMessage> {

    private static final long serialVersionUID = 1L;

    private Logger LOG = LogManager.getLogger(LoadDataBolt.class);

    private CardSettingsRepository cardSettingsRepository;

    private SpendingTotalAmountsRepository totalAmountsRepository;

    private AccountTransactionLogRepository accountTransactionLogRepository;


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
        initializeTransactionRepositories();
    }

    private void initializeCardRepositories() {
        ScyllaParams scyllaParams = ComponentFactory.getConfigurationParams().getCardsScyllaParams();
        String nodeList = scyllaParams.getNodeList();
        String keyspace = scyllaParams.getKeyspace();
        cardSettingsRepository = new CardSettingsRepositoryImpl(nodeList, keyspace);
        totalAmountsRepository = new SpendingTotalAmountsRepositoryImpl(nodeList, keyspace);
    }

    private void initializeTransactionRepositories() {
        ScyllaParams scyllaParams = ComponentFactory.getConfigurationParams().getTransactionLogScyllaParams();
        String nodeList = scyllaParams.getNodeList();
        String keyspace = scyllaParams.getKeyspace();
        accountTransactionLogRepository = new AccountTransactionLogRepositoryImpl(nodeList, keyspace);
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA, Fields.CARD_SETTINGS, Fields.TRANSACTION_LOG, Fields.SPENDING_TOTALS));
    }

    @Override
    protected void join(Tuple input, String key, String processId, AuthorisationMessage eventData) {

        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        LOG.info("{}Previous starting processing the join data load for key {}", logPrefix, key);

        try {
            long cardId = eventData.getDebitCardId();
            SpendGroup totalType = eventData.getSpendGroup();

            CompletableFuture<CardSettings> settingsFuture = retrieveCardSettings(cardId, logPrefix);
            CompletableFuture<AccountTransactionLog> accountTransactionLogFuture =
                    retrieveAccountTransactionLog(settingsFuture, logPrefix);
            CompletableFuture<SpendingTotalAmounts> totalFuture =
                    retrieveTotalAmounts(cardId, totalType, new Date(), logPrefix);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_DATA, eventData);
            values.put(Fields.CARD_SETTINGS, settingsFuture.get());
            values.put(Fields.TRANSACTION_LOG, accountTransactionLogFuture.get());
            values.put(Fields.SPENDING_TOTALS, totalFuture.get());

            send(input, values);

        } catch (Exception e) {
            LOG.error("{}Error processing the authorisation data load. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    private CompletableFuture<CardSettings> retrieveCardSettings(Long cardId, String logPrefix) {
        return CompletableFuture.supplyAsync(
                () -> {
                    LOG.info("{}Retrieving card settings for debitCardId={} ...", logPrefix, cardId);
                    CardSettings cardSettings = cardSettingsRepository.getCardSettings(cardId);
                    LOG.info("{}Card settings retrieved for debitCardId={}: {}", logPrefix, cardId, cardSettings);
                    return cardSettings;
                });
    }

    private CompletableFuture<AccountTransactionLog> retrieveAccountTransactionLog(CompletableFuture<CardSettings> settingsFuture, String logPrefix) {
        return settingsFuture.thenApply(settings -> {
            if (settings == null){
                LOG.info("{}No card settings - cannot retrieve linked account transaction log", logPrefix);
                return null;
            }
            Long linkedAccountId = settings.getLinkedAccountId();
            LOG.info("{}Retrieving account transaction log for account id {} ...", logPrefix, linkedAccountId);
            AccountTransactionLog transactionLog = accountTransactionLogRepository.getLastByAccountId(linkedAccountId.toString());
            LOG.info("{}Account transaction log retrieved for account id {}: {}", logPrefix, linkedAccountId, transactionLog);
            return transactionLog;
        });
    }

    private CompletableFuture<SpendingTotalAmounts> retrieveTotalAmounts(long cardId, SpendGroup totalType, Date date, String logPrefix) {
        return CompletableFuture.supplyAsync(
                () -> {
                    LOG.info(
                            "{}Retrieving total transaction amounts for debitCardId={}, totalType={} ...",
                            logPrefix, cardId, totalType);
                    SpendingTotalAmounts totalAmounts = totalAmountsRepository.getTotalAmounts(cardId, totalType);
                    LOG.info("{}Total transaction amounts retrieved for debitCardId={}, totalType={}: {}",
                            logPrefix, cardId, totalType, totalAmounts);
                    return totalAmounts;
                });
    }
}
