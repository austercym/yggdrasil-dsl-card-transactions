package com.yggdrasil.dsl.card.transactions.topology.bolts.processors;

import com.orwellg.umbrella.avro.types.command.accounting.*;
import com.orwellg.umbrella.avro.types.commons.TransactionType;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.yggdrasil.commons.factories.ClusterFactory;
import com.orwellg.yggdrasil.commons.net.Cluster;
import com.orwellg.yggdrasil.commons.net.Node;
import com.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class AccountingBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(AccountingBolt.class);
    private Cluster processorCluster;

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        processorCluster = ClusterFactory.createCluster(ComponentFactory.getConfigurationParams().getNetworkConfig());
        //todo: what to insert into zookeeper?
    }

    @Override
    public void declareFieldsDefinition() {
        //todo: error stream?
        addFielsDefinition(Arrays.asList(new String[] {"key", "processId", "commandName", "result", "topic"}));
    }

    @Override
    public void execute(Tuple tuple) {

        try {
            //the balance check has been done before - what to do if the balance has decreased since then?
            //1.debit client total
            //2.credit wirecard available
            //3.credit fees available
            //4.

            List<Object> inputValues = tuple.getValues();
            String key = (String) inputValues.get(0);
            String processId = (String) inputValues.get(1);
            GpsMessageProcessed eventData = (GpsMessageProcessed) inputValues.get(2);

            //check which topic to publish accounting command to
            LOG.info("{}Previous to assign the processor node for product with key {}", key);
            Node processorNode = processorCluster.nodeByAccount(eventData.getInternalAccountId());
            LOG.info("{}Proccesor node acquired, processor node id {} and topic name {} for product with key {}", processorNode.id(), processorNode.getTopic(), key);

            //todo: get wirecard account

            //generate command
            AccountingCommandData commandData = generateCommand(eventData);

            //CommandTypes.ACCOUNTING_COMMAND_RECEIVED.getCommandName()
            Map<String, Object> values = new HashMap<>();
            values.put("key", key);
            values.put("processId", processId);
            values.put("commandName", eventData);
            values.put("result", commandData);
            values.put("topic", processorNode.getTopic());

            send(tuple, values);

        }catch(Exception e)
        {
            LOG.error("{} Error processing the gpsMessageProcessed event. Message: {},", e.getMessage(), e);
            error(e, tuple);
        }
    }

    private AccountingCommandData generateCommand(GpsMessageProcessed gpsMessageProcessed) {

        AccountingCommandData commandData = new AccountingCommandData();
        AccountInfo clientAccountInfo = new AccountInfo();
        //clientAccountInfo.setCreditAccountId(2l); //todo: set here a wirecard account id
        //clientAccountInfo.setCreditAccountNumber(); //if needed go to AccountIBAN bolt
        clientAccountInfo.setDebitAccountId(gpsMessageProcessed.getInternalAccountId()); //client account
        //clientAccountInfo.setDebitAccountNumber(); //account identification

        // TODO: Change this
        //commandData.setProductInfo(new ProductInfo());
        //commandData.getProductInfo().setId(String.valueOf(product.getProductId()));
        //commandData.getProductInfo().setDescription(product.getProductFeatures());

        commandData.setTransactionInfo(new TransactionInfo());
        commandData.getTransactionInfo().setAmount(gpsMessageProcessed.getWirecardAmount());
        commandData.getTransactionInfo().setCurrency(gpsMessageProcessed.getWirecardCurrency());
        commandData.getTransactionInfo().setData(gpsMessageProcessed.getGpsTransactionLink());
        commandData.getTransactionInfo().setId(gpsMessageProcessed.getGpsTransactionId());
        commandData.getTransactionInfo().setDirection(TransactionDirection.OUTGOING);//todo: is card transaction outgoing or incoming?
        Boolean isDebit = gpsMessageProcessed.getWirecardAmount() > 0;
        if (isDebit) {
            commandData.getTransactionInfo().setTransactionType(TransactionType.DEBIT); //todo: depends on presentment sign..
        }else {
            commandData.getTransactionInfo().setTransactionType(TransactionType.CREDIT); //todo: depends on presentment sign..
        }
        return commandData;
    }




}
