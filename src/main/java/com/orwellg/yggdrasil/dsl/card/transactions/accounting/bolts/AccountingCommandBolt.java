package com.orwellg.yggdrasil.dsl.card.transactions.accounting.bolts;

import com.google.gson.Gson;
import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.avro.types.command.accounting.*;
import com.orwellg.umbrella.avro.types.commons.TransactionType;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.umbrella.commons.utils.enums.CommandTypes;
import com.orwellg.umbrella.commons.utils.enums.KafkaHeaders;
import com.orwellg.umbrella.commons.utils.enums.Systems;
import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfig;
import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfigFactory;
import com.orwellg.yggdrasil.commons.factories.ClusterFactory;
import com.orwellg.yggdrasil.commons.net.Cluster;
import com.orwellg.yggdrasil.commons.net.Node;
import com.orwellg.yggdrasil.commons.utils.enums.SpecialAccountTypes;
import com.orwellg.yggdrasil.dsl.card.transactions.accounting.AccountingTopology;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.math.BigDecimal;
import java.util.*;

import static com.orwellg.yggdrasil.dsl.card.transactions.common.AccountingTagsGenerator.getAccountingTags;

public class AccountingCommandBolt extends BasicRichBolt {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(AccountingCommandBolt.class);

    private Cluster processorCluster;
    private Gson gson;
    private String accountingResponseTopic;

    void setProcessorCluster(Cluster processorCluster) {
        this.processorCluster = processorCluster;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        String zookeeperHost = (String) stormConf.get(Constants.ZK_HOST_LIST_PROPERTY);
        TopologyConfig topologyConfig = TopologyConfigFactory.getTopologyConfig(
                AccountingTopology.PROPERTIES_FILE, zookeeperHost);
        accountingResponseTopic = topologyConfig
                .getKafkaPublisherBoltConfig()
                .getTopic().getName().get(0);
        initialiseProcessorCluster(topologyConfig);
        this.gson = new Gson();
    }

    protected void initialiseProcessorCluster(TopologyConfig topologyConfig) {
        processorCluster = ClusterFactory.createCluster(topologyConfig.getNetworkConfig());
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.RESULT,
                Fields.COMMAND_NAME, Fields.COMMAND_KEY,
                Fields.TOPIC, Fields.HEADERS));
        addFielsDefinition(AccountingTopology.NO_ACCOUNTING_STREAM, Arrays.asList(
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
                            processId,
                            logPrefix);
                } else if (isClientCredit(processed)) {
                    LOG.info("{}Credit client", logPrefix);
                    command = generateCommand(
                            processed,
                            wirecardAccountId,
                            processed.getInternalAccountId(),
                            TransactionType.CREDIT,
                            processId,
                            logPrefix);
                }
                LOG.info("{}Accounting command created: {}", logPrefix, command);

                String processorNodeTopic = processorNode.getTopic();
                LOG.info("{}Processor node topic: {}", logPrefix, processorNodeTopic);

                Map<String, Object> values = new HashMap<>();
                values.put(Fields.KEY, key);
                values.put(Fields.PROCESS_ID, processId);
                values.put(Fields.RESULT, command);
                values.put(Fields.COMMAND_KEY, processId);
                values.put(Fields.COMMAND_NAME, CommandTypes.ACCOUNTING_COMMAND_RECEIVED.getCommandName());
                values.put(Fields.TOPIC, processorNodeTopic);
                values.put(Fields.HEADERS, addHeaderValue(
                        headers, KafkaHeaders.REPLY_TO.getKafkaHeader(), accountingResponseTopic.getBytes()));
                LOG.info("{}Reply-To header: {}", logPrefix, accountingResponseTopic);
                send(input, values);
            } else {
                LOG.info("{}No accounting operation required", logPrefix);

                Map<String, Object> values = new HashMap<>();
                values.put(Fields.KEY, key);
                values.put(Fields.PROCESS_ID, processId);
                values.put(Fields.EVENT_NAME, CardTransactionEvents.ACCOUNTING_COMPLETED.getEventName());
                values.put(Fields.RESULT, processed);
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
            TransactionType transactionType, String processId, String logPrefix) {

        LOG.info("{}Generating accounting command with id: {}", logPrefix, processId);

        AccountingCommandData commandData = new AccountingCommandData();
        commandData.setAccountingInfo(new AccountingInfo());
        commandData.setEntryOrigin(Systems.CARDS_GPS.getSystem());

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

        commandData.setAccountingTags(getAccountingTags(processId, processed));
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
