package com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.processors.authorisation;

import com.orwellg.umbrella.commons.repositories.CardSettingsRepository;
import com.orwellg.umbrella.commons.repositories.SpendingTotalAmountsRepository;
import com.orwellg.umbrella.commons.repositories.scylla.CardSettingsRepositoryImpl;
import com.orwellg.umbrella.commons.repositories.scylla.SpendingTotalAmountsRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.JoinFutureBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendGroup;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.yggdrasil.dsl.card.transactions.model.AuthorisationMessage;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class LoadDataBolt extends JoinFutureBolt<AuthorisationMessage> {

    private static final long serialVersionUID = 1L;

    private Logger LOG = LogManager.getLogger(LoadDataBolt.class);

    private CardSettingsRepository cardSettingsRepository;

    private SpendingTotalAmountsRepository totalAmountsRepository;

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
        String nodeList = ComponentFactory.getConfigurationParams().getScyllaConfig().getScyllaParams().getNodeList();
        String keyspace = ComponentFactory.getConfigurationParams().getScyllaConfig().getScyllaParams().getKeyspace();
        LOG.info("Repository configuration - NodeList={}, KeySpace={}", nodeList, keyspace);
        cardSettingsRepository = new CardSettingsRepositoryImpl(nodeList, keyspace);
        totalAmountsRepository = new SpendingTotalAmountsRepositoryImpl(nodeList, keyspace);
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                "key", "processId", "eventData", "cardSettings", "spendingTotals"));
    }

    @Override
    protected void join(Tuple input, String key, String processId, AuthorisationMessage eventData) {

        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        LOG.info("{}Previous starting processing the join data load for key {}", logPrefix, key);

        try {
            long cardId = eventData.getDebitCardId();
            SpendGroup totalType = eventData.getSpendGroup();

            CompletableFuture<CardSettings> settingsFuture = retrieveCardSettings(cardId, logPrefix);
            CompletableFuture<SpendingTotalAmounts> totalFuture =
                    retrieveTotalAmounts(cardId, totalType, new Date(), logPrefix);

            Map<String, Object> values = new HashMap<>();
            values.put("key", key);
            values.put("processId", processId);
            values.put("eventData", eventData);
            values.put("cardSettings", settingsFuture.get());
            values.put("spendingTotals", totalFuture.get());

            send(input, values);

        } catch (Exception e) {
            LOG.error("{} Error processing the authorisation data load. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    private CompletableFuture<CardSettings> retrieveCardSettings(Long cardId, String logPrefix) {
        return CompletableFuture.supplyAsync(
                () -> {
                    LOG.info("{}{} Retrieving card settings for debitCardId={} ...", logPrefix, cardId);
                    CardSettings cardSettings = cardSettingsRepository.getCardSettings(cardId);
                    LOG.info("{}{} Card settings retrieved for debitCardId={}: {}", logPrefix, cardId, cardSettings);
                    return cardSettings;
                });
    }

    private CompletableFuture<SpendingTotalAmounts> retrieveTotalAmounts(long cardId, SpendGroup totalType, Date date, String logPrefix) {
        return CompletableFuture.supplyAsync(
                () -> {
                    LOG.info(
                            "{}{} Retrieving total transaction amounts for debitCardId={}, totalType={} ...",
                            logPrefix, cardId, totalType);
                    SpendingTotalAmounts totalAmounts = totalAmountsRepository.getTotalAmounts(cardId, totalType, date);
                    LOG.info("{}{} Total transaction amounts retrieved for debitCardId={}, totalType={}: {}",
                            logPrefix, cardId, totalType, totalAmounts);
                    return totalAmounts;
                });
    }
}
