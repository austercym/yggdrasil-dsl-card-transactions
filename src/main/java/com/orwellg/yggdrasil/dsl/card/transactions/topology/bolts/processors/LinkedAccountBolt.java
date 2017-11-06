package com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.processors;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.repositories.LinkedAccountRepository;
import com.orwellg.umbrella.commons.repositories.scylla.LinkedAccountRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.LinkedAccount;
import com.orwellg.yggdrasil.dsl.card.transactions.GpsMessage;
import com.orwellg.yggdrasil.dsl.card.transactions.topology.CardPresentmentDSLTopology;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import java.util.*;
import org.apache.logging.log4j.Logger;

public class LinkedAccountBolt extends BasicRichBolt {


    private LinkedAccountRepository repository;
    private Logger LOG = LogManager.getLogger(LinkedAccountBolt.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "retrieveValue", "gpsMessage"));
        addFielsDefinition(CardPresentmentDSLTopology.ERROR_STREAM, Arrays.asList("key", "processId", "eventData"));
    }

    protected void setScyllaConnectionParameters() {
        setScyllaNodes(ComponentFactory.getConfigurationParams().getScyllaConfig().getScyllaParams().getNodeList());
        setScyllaKeyspace(ComponentFactory.getConfigurationParams().getScyllaConfig().getScyllaParams().getKeyspace());
    }

    protected List<LinkedAccount> retrieve(Message message, GpsMessage presentment) {
        long cardTransactionId = Long.parseLong(message.getCustRef());
        List<LinkedAccount> linkedAccount = repository.getLinkedAccountByDate(cardTransactionId,
                    presentment.getTransactionTimestamp());
        return linkedAccount;
    }


    @Override
    public void execute(Tuple input) {
        try {
            Map<String, Object> values = new HashMap<>();
            values.put("key", input.getStringByField("key"));
            values.put("processId", input.getStringByField("processId"));
            values.put("eventData", input.getValueByField("eventData"));
            values.put("gpsMessage", input.getValueByField("gpsMessage"));
            values.put("retrieveValue", retrieve((Message)input.getValueByField("eventData"),(GpsMessage) input.getValueByField("gpsMessage")));

            send(input, values);
        } catch (Exception e) {
            LOG.error("Error retrieving fee schema history information. Message: {}", input, e.getMessage(), e);

            Map<String, Object> values = new HashMap<>();
            values.put("key", input.getValueByField("key"));
            values.put("processId", input.getValueByField("processId"));
            values.put("eventData", input.getValueByField("eventData"));

            send(CardPresentmentDSLTopology.ERROR_STREAM, input, values);
            LOG.info("Error when retrieving LinkedAccount from database - error send to corresponded kafka topic. Tuple: {}, Message: {}, Error: {}", input, e.getMessage(), e);
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        setScyllaConnectionParameters();
        repository = new LinkedAccountRepositoryImpl(getScyllaNodes(), getScyllaKeyspace());
    }



    private String scyllaNodes;
    private String scyllaKeyspace;
    public String getScyllaNodes() { return scyllaNodes; }
    public String getScyllaKeyspace() { return scyllaKeyspace; }
    public void setScyllaNodes(String scyllaNodes) {	this.scyllaNodes = scyllaNodes; }
    public void setScyllaKeyspace(String scyllaKeyspace) { this.scyllaKeyspace = scyllaKeyspace; }
}
