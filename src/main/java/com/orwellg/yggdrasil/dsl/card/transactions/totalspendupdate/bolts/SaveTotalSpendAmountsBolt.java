package com.orwellg.yggdrasil.dsl.card.transactions.totalspendupdate.bolts;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.commons.config.params.ScyllaParams;
import com.orwellg.umbrella.commons.repositories.scylla.SpendingTotalAmountsRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.SpendingTotalAmountsRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
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
        setScyllaConnectionParameters();
    }

    private void setScyllaConnectionParameters() {
        ScyllaParams scyllaParams = TopologyConfigFactory.getTopologyConfig(propertyFile)
                .getScyllaConfig().getScyllaParams();
        String nodeList = scyllaParams.getNodeList();
        String keyspace = scyllaParams.getKeyspaceCardsDB();
        amountsRepository = new SpendingTotalAmountsRepositoryImpl(nodeList, keyspace);
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
            MessageProcessed eventData = (MessageProcessed) input.getValueByField(Fields.EVENT_DATA);
            SpendingTotalAmounts newSpendAmounts = (SpendingTotalAmounts) input.getValueByField(Fields.NEW_TOTAL_SPEND_AMOUNTS);

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
