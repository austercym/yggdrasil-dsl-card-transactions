package com.orwellg.yggdrasil.dsl.card.transactions.earmarking.bolts;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.avro.types.command.accounting.*;
import com.orwellg.umbrella.avro.types.commons.TransactionType;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.umbrella.commons.utils.enums.CommandTypes;
import com.orwellg.umbrella.commons.utils.enums.KafkaHeaders;
import com.orwellg.umbrella.commons.utils.enums.Systems;
import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfigFactory;
import com.orwellg.yggdrasil.commons.factories.ClusterFactory;
import com.orwellg.yggdrasil.commons.net.Cluster;
import com.orwellg.yggdrasil.commons.net.Node;
import com.orwellg.yggdrasil.commons.utils.enums.SpecialAccountTypes;
import com.orwellg.yggdrasil.dsl.card.transactions.earmarking.EarmarkingTopology;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.math.BigDecimal;
import java.util.*;

public class EarmarkingCommandBolt extends BasicRichBolt {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(EarmarkingCommandBolt.class);

    private Cluster processorCluster;
    private Gson gson;

    void setProcessorCluster(Cluster processorCluster) {
        this.processorCluster = processorCluster;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        processorCluster = ClusterFactory.createCluster(
                TopologyConfigFactory.getTopologyConfig(EarmarkingTopology.PROPERTIES_FILE).getNetworkConfig());
        this.gson = new Gson();
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.RESULT,
                Fields.COMMAND_NAME, Fields.COMMAND_KEY,
                Fields.TOPIC, Fields.HEADERS));
        addFielsDefinition(EarmarkingTopology.NO_EARMARKING_STREAM, Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_NAME, Fields.RESULT));
    }

    @Override
    public void execute(Tuple input) {
        String logPrefix = null;
        try {
            String key = input.getStringByField(Fields.KEY);
            String processId = input.getStringByField(Fields.PROCESS_ID);
            MessageProcessed processed = (MessageProcessed) input.getValueByField(Fields.EVENT_DATA);
            logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);
            String headers = input.getStringByField(Fields.HEADERS);
            String accountingResponseTopic = TopologyConfigFactory
                    .getTopologyConfig(EarmarkingTopology.PROPERTIES_FILE)
                    .getKafkaPublisherBoltConfig()
                    .getTopic().getName().get(0);

            if (isEarmarkingOperation(processed)) {
                LOG.debug("{}Generating accounting command", logPrefix);
                Node processorNode = getProcessorNode(processed.getInternalAccountId());
                String wirecardAccountId = processorNode.getSpecialAccount(SpecialAccountTypes.GPS.getLiteral());

                AccountingCommandData command = null;
                if (isPutEarmark(processed)) {
                    LOG.info("{}Block available balance", logPrefix);

                    command = generateCommand(
                            processed,
                            processed.getInternalAccountId(),
                            BalanceUpdateType.AVAILABLE,
                            wirecardAccountId,
                            BalanceUpdateType.NONE,
                            TransactionType.DEBIT,
                            processId);
                } else if (isReleaseEarmark(processed)) {
                    LOG.info("{}Release available balance", logPrefix);
                    command = generateCommand(
                            processed,
                            wirecardAccountId,
                            BalanceUpdateType.NONE,
                            processed.getInternalAccountId(),
                            BalanceUpdateType.AVAILABLE,
                            TransactionType.CREDIT,
                            processId);
                }
                LOG.info("{}Accounting command created: {}", logPrefix, command);

                Map<String, Object> values = new HashMap<>();
                values.put(Fields.KEY, key);
                values.put(Fields.PROCESS_ID, processId);
                values.put(Fields.RESULT, command);
                values.put(Fields.COMMAND_KEY, processId);
                values.put(Fields.COMMAND_NAME, CommandTypes.ACCOUNTING_COMMAND_RECEIVED.getCommandName());
                values.put(Fields.TOPIC, processorNode.getTopic());
                values.put(Fields.HEADERS, addHeaderValue(
                        headers, KafkaHeaders.REPLY_TO.getKafkaHeader(), accountingResponseTopic.getBytes()));
                send(input, values);
            } else {
                LOG.info("{}No earmark operation required", logPrefix);

                Map<String, Object> values = new HashMap<>();
                values.put(Fields.KEY, key);
                values.put(Fields.PROCESS_ID, processId);
                values.put(Fields.EVENT_NAME, CardTransactionEvents.EARMARKING_COMPLETED.getEventName());
                values.put(Fields.RESULT, processed);
                send(EarmarkingTopology.NO_EARMARKING_STREAM, input, values);
            }
        } catch (Exception e) {
            LOG.error("{}Error. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    private boolean isEarmarkingOperation(MessageProcessed processed) {
        return processed.getEarmarkAmount() != null
                && processed.getEarmarkAmount().getValue().compareTo(BigDecimal.ZERO) != 0;
    }

    private boolean isPutEarmark(MessageProcessed processed) {
        return processed.getEarmarkAmount() != null
                && processed.getEarmarkAmount().getValue().compareTo(BigDecimal.ZERO) < 0;
    }

    private boolean isReleaseEarmark(MessageProcessed processed) {
        return processed.getEarmarkAmount() != null
                && processed.getEarmarkAmount().getValue().compareTo(BigDecimal.ZERO) > 0;
    }

    private AccountingCommandData generateCommand(
            MessageProcessed processed,
            String debitAccount, BalanceUpdateType debitAccountUpdateType,
            String creditAccount, BalanceUpdateType creditAccountUpdateType,
            TransactionType transactionType, String processId) {
        AccountingCommandData commandData = new AccountingCommandData();
        commandData.setAccountingInfo(new AccountingInfo());

        AccountInfo debitAccountInfo = new AccountInfo();
        debitAccountInfo.setAccountId(debitAccount);
        commandData.getAccountingInfo().setDebitAccount(debitAccountInfo);
        commandData.getAccountingInfo().setDebitBalanceUpdate(debitAccountUpdateType);

        AccountInfo creditAccountInfo = new AccountInfo();
        creditAccountInfo.setAccountId(creditAccount);
        commandData.getAccountingInfo().setCreditAccount(creditAccountInfo);
        commandData.getAccountingInfo().setCreditBalanceUpdate(creditAccountUpdateType);

        commandData.setTransactionInfo(new TransactionAccountingInfo());
        commandData.getTransactionInfo().setAmount(DecimalTypeUtils.toDecimal(
                processed.getEarmarkAmount().getValue().abs()));
        commandData.getTransactionInfo().setCurrency(processed.getEarmarkCurrency());
        commandData.getTransactionInfo().setId(processId);
        commandData.getTransactionInfo().setSystem(Systems.CARDS_GPS.getSystem());
        commandData.getTransactionInfo().setDirection(TransactionDirection.INTERNAL);
        commandData.getTransactionInfo().setTransactionType(transactionType);
        commandData.getTransactionInfo().setData(gson.toJson(processed));
        return commandData;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private Node getProcessorNode(String internalAccountId) {
        Node processorNode;
        if (StringUtils.isNotBlank(internalAccountId)) {
            processorNode = processorCluster.nodeByAccount(internalAccountId);
        } else {
            Map nodes = processorCluster.getNodes();
            List<Node> listNodes = new ArrayList<Node>(nodes.values());
            Integer randomNode = new Random().nextInt((listNodes.size() - 1) + 1);
            LOG.info("get random processor Node for exception processing {}", randomNode);
            processorNode = listNodes.get(randomNode);
        }
        return processorNode;
    }
}
