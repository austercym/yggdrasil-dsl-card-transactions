package com.orwellg.yggdrasil.dsl.card.transactions.topology.bolts.processors;

import com.orwellg.umbrella.avro.types.command.accounting.*;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.yggdrasil.commons.factories.ClusterFactory;
import com.orwellg.yggdrasil.commons.net.Cluster;
import com.orwellg.yggdrasil.commons.net.Node;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
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
        AccountingInfo accountingInfo = new AccountingInfo();

        AccountInfo clientAccount = new AccountInfo();
        clientAccount.setAccountId(gpsMessageProcessed.getInternalAccountId().toString());
        //clientAccount.setAccountNumber(); todo
        AccountInfo wirecardAccount = new AccountInfo(); //todo: get wirecard account

        return commandData;
    }

    private void setWirecardAccountingData(GpsMessageProcessed gpsMessageProcessed, AccountInfo clientAccount, AccountInfo wirecardAccount){

        //1. check what we need to apply to wirecard
        //if 0 - just create ledger or available balance with same amount as before - dependent on debit or credit
        //if something else - create adjustment in balances: will need to apply two different rates
        //or revert previous balances and create new ones?

        AccountingInfo accountingInfo = new AccountingInfo();
        TransactionAccountingInfo transactionAccountingInfo = new TransactionAccountingInfo();

    }





}
