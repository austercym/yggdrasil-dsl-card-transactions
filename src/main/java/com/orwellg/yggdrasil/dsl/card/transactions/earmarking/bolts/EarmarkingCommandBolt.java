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
import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfig;
import com.orwellg.yggdrasil.card.transaction.commons.config.TopologyConfigFactory;
import com.orwellg.yggdrasil.commons.factories.ClusterFactory;
import com.orwellg.yggdrasil.commons.net.Cluster;
import com.orwellg.yggdrasil.commons.net.Node;
import com.orwellg.yggdrasil.commons.utils.enums.SpecialAccountTypes;
import com.orwellg.yggdrasil.dsl.card.transactions.earmarking.EarmarkingTopology;
import com.orwellg.yggdrasil.net.client.producer.CommandProducerConfig;
import com.orwellg.yggdrasil.net.client.producer.GeneratorIdCommandProducer;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.*;

import static com.orwellg.yggdrasil.dsl.card.transactions.common.AccountingTagsGenerator.getAccountingTags;

public class EarmarkingCommandBolt extends BasicRichBolt {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LogManager.getLogger(EarmarkingCommandBolt.class);

    private Cluster processorCluster;
    private GeneratorIdCommandProducer idGenerator;
    private Gson gson;

    void setProcessorCluster(Cluster processorCluster) {
        this.processorCluster = processorCluster;
    }

    void setIdGenerator(GeneratorIdCommandProducer idGenerator) {
        this.idGenerator = idGenerator;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        TopologyConfig topologyConfig = TopologyConfigFactory.getTopologyConfig(EarmarkingTopology.PROPERTIES_FILE);
        initialiseProcessorCluster(topologyConfig);
        initialiseIdGeneratorClient(topologyConfig);
        this.gson = new Gson();
    }

    protected void initialiseProcessorCluster(TopologyConfig topologyConfig) {
        setProcessorCluster(ClusterFactory.createCluster(
                topologyConfig.getNetworkConfig()));
    }

    private void initialiseIdGeneratorClient(TopologyConfig topologyConfig) {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name());
        props.setProperty(CommandProducerConfig.BOOTSTRAP_SERVERS_CONFIG, topologyConfig.getZookeeperConnection());
        props.setProperty(CommandProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(CommandProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        setIdGenerator(new GeneratorIdCommandProducer(new CommandProducerConfig(props), 1, Time.SYSTEM));
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
                String accountingId = generateId(logPrefix);
                if (isPutEarmark(processed)) {
                    LOG.info("{}Block available balance", logPrefix);

                    command = generateCommand(
                            processed,
                            processed.getInternalAccountId(),
                            BalanceUpdateType.AVAILABLE,
                            wirecardAccountId,
                            BalanceUpdateType.NONE,
                            TransactionType.DEBIT,
                            processId,
                            accountingId,
                            logPrefix);
                } else if (isReleaseEarmark(processed)) {
                    LOG.info("{}Release available balance", logPrefix);
                    command = generateCommand(
                            processed,
                            wirecardAccountId,
                            BalanceUpdateType.NONE,
                            processed.getInternalAccountId(),
                            BalanceUpdateType.AVAILABLE,
                            TransactionType.CREDIT,
                            processId,
                            accountingId,
                            logPrefix);
                }
                LOG.info("{}Accounting command created: {}", logPrefix, command);

                String processorNodeTopic = processorNode.getTopic();
                LOG.info("{}Processor node topic: {}", logPrefix, processorNodeTopic);

                Map<String, Object> values = new HashMap<>();
                values.put(Fields.KEY, key);
                values.put(Fields.PROCESS_ID, processId);
                values.put(Fields.RESULT, command);
                values.put(Fields.COMMAND_KEY, accountingId);
                values.put(Fields.COMMAND_NAME, CommandTypes.ACCOUNTING_COMMAND_RECEIVED.getCommandName());
                values.put(Fields.TOPIC, processorNodeTopic);
                values.put(Fields.HEADERS, addHeaderValue(
                        headers, KafkaHeaders.REPLY_TO.getKafkaHeader(), accountingResponseTopic.getBytes()));
                LOG.info("{}Reply-To header: {}", logPrefix, accountingResponseTopic);
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
            TransactionType transactionType, String processId, String accountingId, String logPrefix) {

        LOG.info("{}Generating earmark accounting command with id: {}", logPrefix, accountingId);

        AccountingCommandData commandData = new AccountingCommandData();
        commandData.setAccountingInfo(new AccountingInfo());
        commandData.setEntryOrigin(Systems.CARDS_GPS.getSystem());

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
        commandData.getTransactionInfo().setId(accountingId);
        commandData.getTransactionInfo().setSystem(Systems.CARDS_GPS.getSystem());
        commandData.getTransactionInfo().setDirection(TransactionDirection.INTERNAL);
        commandData.getTransactionInfo().setTransactionType(transactionType);
        commandData.getTransactionInfo().setData(gson.toJson(processed));

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

    private String generateId(String logPrefix) {
        try {
            return idGenerator.getGeneralUniqueId();
        } catch (Exception e) {
            LOG.warn(
                    "{}Id generator client exception, a random id has been generated",
                    logPrefix, e);
            return UUID.randomUUID().toString() + "_" + Instant.now().hashCode();
        }
    }
}
