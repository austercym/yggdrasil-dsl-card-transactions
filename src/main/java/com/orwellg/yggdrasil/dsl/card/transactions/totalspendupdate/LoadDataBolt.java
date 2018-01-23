package com.orwellg.yggdrasil.dsl.card.transactions.totalspendupdate;

import com.orwellg.umbrella.avro.types.cards.SpendGroup;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.repositories.scylla.SpendingTotalAmountsRepository;
import com.orwellg.umbrella.commons.repositories.scylla.TransactionEarmarksRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.SpendingTotalAmountsRepositoryImpl;
import com.orwellg.umbrella.commons.repositories.scylla.impl.TransactionEarmarksRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.JoinFutureBolt;
import com.orwellg.umbrella.commons.storm.topology.component.spout.KafkaSpout;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionEarmark;
import com.orwellg.yggdrasil.dsl.card.transactions.config.ScyllaParams;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class LoadDataBolt extends JoinFutureBolt<GpsMessageProcessed> {

    private static final long serialVersionUID = 1L;

    private Logger LOG = LogManager.getLogger(LoadDataBolt.class);

    private SpendingTotalAmountsRepository amountsRepository;

    private TransactionEarmarksRepository ermarksRepository;


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
        ermarksRepository = new TransactionEarmarksRepositoryImpl(nodeList, keyspace);
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA, Fields.TOTAL_AMOUNTS, Fields.EARMARK));
    }

    @Override
    protected void join(Tuple input, String key, String processId, GpsMessageProcessed eventData) {

        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        LOG.info("{}Starting processing the total spend data load for key {}", logPrefix, key);

        try {
            CompletableFuture<SpendingTotalAmounts> totalAmountsFuture = retrieveTotalAmounts(
                    eventData.getDebitCardId(), eventData.getSpendGroup(), logPrefix);
            CompletableFuture<TransactionEarmark> earmarkFuture = "P".equalsIgnoreCase(eventData.getGpsMessageType())
                    ? retrieveEarmark(eventData.getGpsTransactionLink(), logPrefix)
                    : CompletableFuture.completedFuture(null);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_DATA, eventData);
            values.put(Fields.TOTAL_AMOUNTS, totalAmountsFuture.get());
            values.put(Fields.EARMARK, earmarkFuture.get());

            send(input, values);

        } catch (Exception e) {
            LOG.error("{}Error processing the total spend update data load. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    private CompletableFuture<TransactionEarmark> retrieveEarmark(String gpsTransactionLink, String logPrefix) {
        return CompletableFuture.supplyAsync(
                () -> {
                    LOG.info("{}Retrieving earmark for GpsTransactionLink={} ...", logPrefix, gpsTransactionLink);
                    TransactionEarmark earmark = ermarksRepository.getEarmark(gpsTransactionLink);
                    LOG.info("{}Earmark retrieved for GpsTransactionLink={}: {}", logPrefix, gpsTransactionLink, earmark);
                    return earmark;
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
