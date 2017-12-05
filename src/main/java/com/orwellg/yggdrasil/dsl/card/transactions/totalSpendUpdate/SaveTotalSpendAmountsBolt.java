package com.orwellg.yggdrasil.dsl.card.transactions.totalSpendUpdate;

import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.repositories.scylla.SpendingTotalAmountsRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.SpendingTotalAmountsRepositoryImpl;
import com.orwellg.umbrella.commons.repositories.scylla.impl.SpendingTotalEarmarksRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalEarmark;
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

public class SaveTotalSpendAmountsBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(SaveTotalSpendAmountsBolt.class);

    private SpendingTotalAmountsRepository amountsRepository;
    private SpendingTotalEarmarksRepositoryImpl earmarksRepository;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        setScyllaConnectionParameters();
    }

    protected void setScyllaConnectionParameters() {
        ScyllaParams scyllaParams = ComponentFactory.getConfigurationParams().getCardsScyllaParams();
        String nodeList = scyllaParams.getNodeList();
        String keyspace = scyllaParams.getKeyspace();
        amountsRepository = new SpendingTotalAmountsRepositoryImpl(nodeList, keyspace);
        earmarksRepository = new SpendingTotalEarmarksRepositoryImpl(nodeList, keyspace);
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA, Fields.NEW_TOTAL_SPEND_AMOUNTS));
    }

    @Override
    public void execute(Tuple input) {
        String key = input.getStringByField(Fields.KEY);
        String processId = input.getStringByField(Fields.PROCESS_ID);
        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        LOG.info("{}Saving total spend amounts", logPrefix);

        try {
            GpsMessageProcessed eventData = (GpsMessageProcessed) input.getValueByField(Fields.EVENT_DATA);
            SpendingTotalAmounts newSpendAmounts = (SpendingTotalAmounts) input.getValueByField(Fields.NEW_TOTAL_SPEND_AMOUNTS);
            SpendingTotalEarmark newEarmark = (SpendingTotalEarmark) input.getValueByField(Fields.NEW_EARMARK);

            if (newEarmark == null) {
                LOG.info("{}No new earmark to save", logPrefix);
            } else {
                earmarksRepository.addEarmark(newEarmark);
            }

            if (newSpendAmounts == null) {
                LOG.info("{}No new spend amounts to save", logPrefix);
            } else {
                amountsRepository.addTotalAmounts(newSpendAmounts);
            }

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_DATA, eventData);
            values.put(Fields.NEW_TOTAL_SPEND_AMOUNTS, newSpendAmounts);
            send(input, values);
        } catch (Exception e) {
            LOG.error("{}Error saving total spend amounts. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }
}
