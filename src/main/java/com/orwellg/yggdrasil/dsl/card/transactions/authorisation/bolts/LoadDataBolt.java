package com.orwellg.yggdrasil.dsl.card.transactions.authorisation.bolts;

import com.datastax.driver.core.Session;
import com.orwellg.umbrella.avro.types.cards.SpendGroup;
import com.orwellg.umbrella.commons.repositories.scylla.AccountBalanceRepository;
import com.orwellg.umbrella.commons.repositories.scylla.CardSettingsRepository;
import com.orwellg.umbrella.commons.repositories.scylla.CardTransactionRepository;
import com.orwellg.umbrella.commons.repositories.scylla.SpendingTotalAmountsRepository;
import com.orwellg.umbrella.commons.repositories.scylla.cards.TransactionMatchingRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.AccountBalanceRepositoryImpl;
import com.orwellg.umbrella.commons.repositories.scylla.impl.CardSettingsRepositoryImpl;
import com.orwellg.umbrella.commons.repositories.scylla.impl.SpendingTotalAmountsRepositoryImpl;
import com.orwellg.umbrella.commons.repositories.scylla.impl.cards.CardTransactionRepositoryImpl;
import com.orwellg.umbrella.commons.repositories.scylla.impl.cards.TransactionMatchingRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.JoinFutureBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.AccountBalance;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.yggdrasil.card.transaction.commons.authorisation.services.AuthorisationDataService;
import com.orwellg.yggdrasil.card.transaction.commons.config.ScyllaSessionFactory;
import com.orwellg.yggdrasil.card.transaction.commons.model.TransactionInfo;
import com.orwellg.yggdrasil.card.transaction.commons.transactionmatching.TransactionMatcher;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class LoadDataBolt extends JoinFutureBolt<TransactionInfo> {

    private static final long serialVersionUID = 1L;

    private Logger LOG = LogManager.getLogger(LoadDataBolt.class);

    private AuthorisationDataService authorisationDataService;
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

        initializeCardRepositories();
    }

    private void initializeCardRepositories() {
        Session session = ScyllaSessionFactory.getSession(propertyFile);
        CardSettingsRepository cardSettingsRepository = new CardSettingsRepositoryImpl(session);
        SpendingTotalAmountsRepository totalAmountsRepository = new SpendingTotalAmountsRepositoryImpl(session);
        AccountBalanceRepository accountBalanceRepository = new AccountBalanceRepositoryImpl(session);
        TransactionMatchingRepository matchingRepository = new TransactionMatchingRepositoryImpl(session);
        CardTransactionRepository transactionRepository = new CardTransactionRepositoryImpl(session);
        TransactionMatcher transactionMatcher = new TransactionMatcher(matchingRepository);
        authorisationDataService = new AuthorisationDataService(
                cardSettingsRepository, totalAmountsRepository, accountBalanceRepository, transactionRepository, transactionMatcher);
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA,
                Fields.CARD_SETTINGS, Fields.ACCOUNT_BALANCE, Fields.SPENDING_TOTALS, Fields.TRANSACTION_ID,
                Fields.TRANSACTION_LIST));
    }

    @Override
    protected void join(Tuple input, String key, String processId, TransactionInfo eventData) {

        long startTime = System.currentTimeMillis();
        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        LOG.info("{}Starting processing the join data load for key {}", logPrefix, key);

        try {
            String cardId = eventData.getDebitCardId();
            SpendGroup totalType = eventData.getSpendGroup();

            CompletableFuture<String> transactionIdFuture = CompletableFuture.supplyAsync(() ->
                    StringUtils.defaultIfEmpty(
                            authorisationDataService.tryMatchTransaction(eventData.getMessage()),
                            processId));
            CompletableFuture<CardSettings> settingsFuture = CompletableFuture.supplyAsync(() ->
                    authorisationDataService.retrieveCardSettings(cardId));
            CompletableFuture<AccountBalance> accountTransactionLogFuture = settingsFuture.thenApply(settings ->
                    authorisationDataService.retrieveAccountBalance(settings));
            CompletableFuture<SpendingTotalAmounts> totalFuture =
                    eventData.getIsBalanceEnquiry()
                            ? CompletableFuture.completedFuture(null)
                            : CompletableFuture.supplyAsync(() ->
                            authorisationDataService.retrieveTotalAmounts(cardId, totalType, new Date()));
            String transactionId = transactionIdFuture.get();
            List<CardTransaction> transactionList = StringUtils.isNotEmpty(transactionId)
                    ? authorisationDataService.retrieveHistory(transactionId)
                    : null;

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_DATA, eventData);
            values.put(Fields.CARD_SETTINGS, settingsFuture.get());
            values.put(Fields.ACCOUNT_BALANCE, accountTransactionLogFuture.get());
            values.put(Fields.SPENDING_TOTALS, totalFuture.get());
            values.put(Fields.TRANSACTION_ID, transactionId);
            values.put(Fields.TRANSACTION_LIST, transactionList);

            send(input, values);

            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            LOG.info("{}Processed the join data load for key {}. (Execution time: {} ms)", logPrefix, key, elapsedTime);
        } catch (Exception e) {
            LOG.error("{}Error processing the authorisation data load. Message: {}", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }
}
