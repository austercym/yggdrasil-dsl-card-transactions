package com.orwellg.yggdrasil.dsl.card.transactions.accounting.bolts;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.avro.types.command.accounting.*;
import com.orwellg.umbrella.avro.types.commons.TransactionType;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.umbrella.commons.utils.enums.CommandTypes;
import com.orwellg.umbrella.commons.utils.enums.Systems;
import com.orwellg.umbrella.commons.utils.enums.TransactionEvents;
import com.orwellg.yggdrasil.commons.factories.ClusterFactory;
import com.orwellg.yggdrasil.commons.net.Cluster;
import com.orwellg.yggdrasil.commons.net.Node;
import com.orwellg.yggdrasil.commons.utils.enums.SpecialAccountTypes;
import com.orwellg.yggdrasil.dsl.card.transactions.accounting.AccountingTopology;
import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfigFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.math.BigDecimal;
import java.util.*;

public class AccountingCommandBolt extends BasicRichBolt {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(AccountingCommandBolt.class);

    private Cluster processorCluster;

    void setProcessorCluster(Cluster processorCluster) {
        this.processorCluster = processorCluster;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        processorCluster = ClusterFactory.createCluster(
                TopologyConfigFactory.getTopologyConfig(AccountingTopology.PROPERTIES_FILE).getNetworkConfig());
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.RESULT,
                Fields.COMMAND_NAME, Fields.COMMAND_KEY,
                Fields.EVENT_NAME, Fields.EVENT_DATA, Fields.TOPIC));
        addFielsDefinition(AccountingTopology.NO_ACCOUNTING_STREAM, Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID));
    }

    @Override
    public void execute(Tuple input) {
        String logPrefix = null;
        try {
            String key = input.getStringByField(Fields.KEY);
            String processId = input.getStringByField(Fields.PROCESS_ID);
            MessageProcessed processed = (MessageProcessed) input.getValueByField(Fields.EVENT_DATA);
            logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

            if (isAccountingRequired(processed)) {
                LOG.debug("{}Generating accounting command", logPrefix);
                Node processorNode = getProcessorNode(processed.getInternalAccountId());
                String wirecardAccountId = processorNode.getSpecialAccount(SpecialAccountTypes.GPS.getLiteral());

                AccountingCommandData command = null;
                if (isClientDebit(processed)) {
                    LOG.info("{}Debit client", logPrefix);

                    command = generateCommand(
                            processed,
                            processed.getInternalAccountId(),
                            wirecardAccountId,
                            TransactionType.DEBIT,
                            processId);
                } else if (isClientCredit(processed)) {
                    LOG.info("{}Credit client", logPrefix);
                    command = generateCommand(
                            processed,
                            wirecardAccountId,
                            processed.getInternalAccountId(),
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
                values.put(Fields.EVENT_NAME, TransactionEvents.INCOMING_TRANSACTION_RECEIVED.getEventName());
                values.put(Fields.EVENT_DATA, processed);
                values.put(Fields.TOPIC, processorNode.getTopic());
                send(input, values);
            } else {
                LOG.info("{}No accounting operation required", logPrefix);

                Map<String, Object> values = new HashMap<>();
                values.put(Fields.KEY, key);
                values.put(Fields.PROCESS_ID, processId);
                send(AccountingTopology.NO_ACCOUNTING_STREAM, input, values);
            }
        } catch (Exception e) {
            LOG.error("{}Error. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    private boolean isAccountingRequired(MessageProcessed processed) {
        return (
                processed.getClientAmount() != null
                        && processed.getClientAmount().getValue().compareTo(BigDecimal.ZERO) != 0
        )
                || (
                processed.getWirecardAmount() != null
                        && processed.getWirecardAmount().getValue().compareTo(BigDecimal.ZERO) != 0
        );
    }

    private boolean isClientDebit(MessageProcessed processed) {
        return processed.getClientAmount() != null && processed.getWirecardAmount() != null
                && processed.getClientAmount().getValue().compareTo(BigDecimal.ZERO) < 0
                && processed.getWirecardAmount().getValue().compareTo(BigDecimal.ZERO) > 0;
    }

    private boolean isClientCredit(MessageProcessed processed) {
        return processed.getClientAmount() != null && processed.getWirecardAmount() != null
                && processed.getClientAmount().getValue().compareTo(BigDecimal.ZERO) > 0
                && processed.getWirecardAmount().getValue().compareTo(BigDecimal.ZERO) < 0;
    }

    private AccountingCommandData generateCommand(
            MessageProcessed processed,
            String debitAccount, String creditAccount,
            TransactionType transactionType, String processId) {
        AccountingCommandData commandData = new AccountingCommandData();
        commandData.setAccountingInfo(new AccountingInfo());

        AccountInfo debitAccountInfo = new AccountInfo();
        debitAccountInfo.setAccountId(debitAccount);
        commandData.getAccountingInfo().setDebitAccount(debitAccountInfo);
        commandData.getAccountingInfo().setDebitBalanceUpdate(BalanceUpdateType.ALL);

        AccountInfo creditAccountInfo = new AccountInfo();
        creditAccountInfo.setAccountId(creditAccount);
        commandData.getAccountingInfo().setCreditAccount(creditAccountInfo);
        commandData.getAccountingInfo().setCreditBalanceUpdate(BalanceUpdateType.ALL);

        commandData.setTransactionInfo(new TransactionAccountingInfo());
        commandData.getTransactionInfo().setAmount(DecimalTypeUtils.toDecimal(
                processed.getClientAmount().getValue().abs()));
        commandData.getTransactionInfo().setCurrency(processed.getClientCurrency());
        commandData.getTransactionInfo().setId(processId);
        commandData.getTransactionInfo().setSystem(Systems.CARDS_GPS.getSystem());
        commandData.getTransactionInfo().setDirection(TransactionDirection.INTERNAL);
        commandData.getTransactionInfo().setTransactionType(transactionType);
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
