package com.orwellg.yggdrasil.dsl.card.transactions.presentment;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.repositories.scylla.LinkedAccountRepository;
import com.orwellg.umbrella.commons.repositories.scylla.impl.LinkedAccountRepositoryImpl;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.LinkedAccount;
import com.orwellg.yggdrasil.dsl.card.transactions.model.PresentmentMessage;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import java.util.*;
import org.apache.logging.log4j.Logger;

public class GetLinkedAccount extends BasicRichBolt {


    private LinkedAccountRepository repository;
    private Logger LOG = LogManager.getLogger(GetLinkedAccount.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "retrieveValue", "gpsMessage"));
    }

    protected void setScyllaConnectionParameters() {
        setScyllaNodes(ComponentFactory.getConfigurationParams().getCardsScyllaParams().getNodeList());
        setScyllaKeyspace(ComponentFactory.getConfigurationParams().getCardsScyllaParams().getKeyspace());
    }

    protected List<LinkedAccount> retrieve(Message message, PresentmentMessage presentment) {
        long cardTransactionId = Long.parseLong(message.getCustRef());
        List<LinkedAccount> linkedAccount = repository.getLinkedAccountByDate(cardTransactionId,
                    presentment.getTransactionTimestamp());
        return linkedAccount;
    }


    @Override
    public void execute(Tuple input) {
        try {

            String key = input.getStringByField("key");
            String processId = input.getStringByField("processId");

            LOG.debug("Key: {} | ProcessId: {} | Retrieving Linked Account from db", key, processId);

            Map<String, Object> values = new HashMap<>();
            values.put("key", key);
            values.put("processId", processId);
            values.put("eventData", input.getValueByField("eventData"));
            values.put("gpsMessage", input.getValueByField("gpsMessage"));
            values.put("retrieveValue", retrieve((Message)input.getValueByField("eventData"),(PresentmentMessage) input.getValueByField("gpsMessage")));
            send(input, values);

        } catch (Exception e) {
            LOG.error("Error retrieving fee schema history information. Message: {}", input, e.getMessage(), e);
            error(e, input);
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
