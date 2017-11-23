package com.orwellg.yggdrasil.dsl.card.transactions.presentment;

import com.orwellg.umbrella.commons.repositories.scylla.FeeHistoryRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.FeeHistoryReposotoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.generics.scylla.ScyllaRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.FeeSchema;
import com.orwellg.yggdrasil.dsl.card.transactions.model.PresentmentMessage;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetFeeSchema extends ScyllaRichBolt<List<FeeSchema>, PresentmentMessage> {

    private FeeHistoryRepository repository;
    private static final Logger LOG = LogManager.getLogger(GetFeeSchema.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "retrieveValue", "gpsMessage"));
        addFielsDefinition(CardPresentmentDSLTopology.ERROR_STREAM, Arrays.asList("key", "processId", "eventData", "exceptionMessage", "exceptionStackTrace"));
    }

    @Override
    protected void setScyllaConnectionParameters() {
        setScyllaNodes(ComponentFactory.getConfigurationParams().getCardsScyllaParams().getNodeList());
        setScyllaKeyspace(ComponentFactory.getConfigurationParams().getCardsScyllaParams().getKeyspace());
    }

    @Override
    protected List<FeeSchema> retrieve(PresentmentMessage presentment) {
        List<FeeSchema> cardSettings =
                repository.getFeeHistory(presentment.getDebitCardId(), presentment.getTransactionTimestamp(),presentment.getFeeTransactionType().toString());
        return cardSettings;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        repository = new FeeHistoryReposotoryImpl(getScyllaNodes(), getScyllaKeyspace());
    }


    @Override
    public void execute(Tuple input) {

        try {

            Map<String, Object> values = new HashMap<>();
            values.put("key", input.getStringByField("key"));
            values.put("processId", input.getStringByField("processId"));
            values.put("eventData", input.getValueByField("eventData"));
            values.put("gpsMessage", input.getValueByField("gpsMessage"));
            values.put("retrieveValue", retrieve((PresentmentMessage) input.getValueByField("gpsMessage")));

            send(input, values);
        } catch (Exception e) {

            LOG.error("Error retrieving fee schema history information. Message: {}", input, e.getMessage(), e);

            Map<String, Object> values = new HashMap<>();
            values.put("key", input.getValueByField("key"));
            values.put("processId", input.getValueByField("processId"));
            values.put("eventData", input.getValueByField("eventData"));
            values.put("exceptionMessage", ExceptionUtils.getMessage(e));
            values.put("exceptionStackTrace", ExceptionUtils.getStackTrace(e));

            send(CardPresentmentDSLTopology.ERROR_STREAM, input, values);
            LOG.info("Error when processing PresentmentMessage - error send to corresponded kafka topic. Tuple: {}, Message: {}, Error: {}", input, e.getMessage(), e);
        }
    }
}
