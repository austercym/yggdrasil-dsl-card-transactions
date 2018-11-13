package com.orwellg.yggdrasil.dsl.card.transactions.totalspendupdate.bolts;

import com.datastax.driver.core.Session;
import com.orwellg.umbrella.commons.config.ScyllaConfig;
import com.orwellg.umbrella.commons.repositories.scylla.SpendingTotalAmountsRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.SpendingTotalAmountsRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
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
import java.util.Map;

public class SaveTotalSpendAmountsBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(SaveTotalSpendAmountsBolt.class);

    private SpendingTotalAmountsRepository amountsRepository;
    private String propertyFile;

    public SaveTotalSpendAmountsBolt(String propertyFile) {
        this.propertyFile = propertyFile;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        String zookeeperHost = (String) stormConf.get(Constants.ZK_HOST_LIST_PROPERTY);
        TopologyConfig topologyConfig = TopologyConfigFactory.getTopologyConfig(propertyFile, zookeeperHost);
        setScyllaConnectionParameters(topologyConfig);
    }

    private void setScyllaConnectionParameters(TopologyConfig topologyConfig) {
        Session session = ScyllaSessionFactory.getCardsSession(topologyConfig.getScyllaConfig());
        amountsRepository = new SpendingTotalAmountsRepositoryImpl(session);
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_NAME, Fields.RESULT));
    }

    @Override
    public void execute(Tuple input) {
        String key = input.getStringByField(Fields.KEY);
        String processId = input.getStringByField(Fields.PROCESS_ID);
        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        LOG.info("{}Saving total spend amounts", logPrefix);

        try {
            SpendingTotalAmounts newSpendAmounts = (SpendingTotalAmounts) input.getValueByField(Fields.NEW_TOTAL_SPEND_AMOUNTS);

            if (newSpendAmounts == null) {
                LOG.info("{}No new spend amounts to save", logPrefix);
            } else {
                amountsRepository.addTotalAmounts(newSpendAmounts);
            }

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_NAME, CardTransactionEvents.TOTAL_SPEND_UPDATED.getEventName());
            values.put(Fields.RESULT, newSpendAmounts);
            send(input, values);
        } catch (Exception e) {
            LOG.error("{}Error saving total spend amounts. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }
}
