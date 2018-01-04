package com.orwellg.yggdrasil.dsl.card.transactions.authorisationReversal;

import com.orwellg.umbrella.commons.repositories.scylla.TransactionEarmarksRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.TransactionEarmarksRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionEarmark;
import com.orwellg.yggdrasil.dsl.card.transactions.config.ScyllaParams;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class LoadDataBolt extends BasicRichBolt {

    private static final long serialVersionUID = 1L;

    private Logger LOG = LogManager.getLogger(LoadDataBolt.class);

    private TransactionEarmarksRepository ermarksRepository;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);

        initializeCardRepositories();
    }

    private void initializeCardRepositories() {
        ScyllaParams scyllaParams = ComponentFactory.getConfigurationParams().getCardsScyllaParams();
        String nodeList = scyllaParams.getNodeList();
        String keyspace = scyllaParams.getKeyspace();
        ermarksRepository = new TransactionEarmarksRepositoryImpl(nodeList, keyspace);
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA, Fields.EARMARK));
    }

    @Override
    public void execute(Tuple input) {
        String key = input.getStringByField(Fields.KEY);
        String processId = input.getStringByField(Fields.PROCESS_ID);
        TransactionInfo eventData = (TransactionInfo) input.getValueByField(Fields.EVENT_DATA);
        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        LOG.info("{}Retrieving earmark for key {}", logPrefix, key);

        try {
            TransactionEarmark earmark = getEarmark(eventData.getGpsTransactionLink(), logPrefix);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_DATA, eventData);
            values.put(Fields.EARMARK, earmark);

            send(input, values);

        } catch (Exception e) {
            LOG.error("{}Error retrieving earmark. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    private TransactionEarmark getEarmark(String gpsTransactionLink, String logPrefix) {
        LOG.info("{}Retrieving earmark for GpsTransactionLink={} ...", logPrefix, gpsTransactionLink);
        TransactionEarmark earmark = ermarksRepository.getEarmark(gpsTransactionLink);
        LOG.info("{}Earmark retrieved for GpsTransactionLink={}: {}", logPrefix, gpsTransactionLink, earmark);
        return earmark;
    }
}
