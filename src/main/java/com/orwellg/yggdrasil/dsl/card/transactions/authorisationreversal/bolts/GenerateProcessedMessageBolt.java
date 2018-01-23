package com.orwellg.yggdrasil.dsl.card.transactions.authorisationreversal.bolts;

import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionEarmark;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.GpsMessageProcessedFactory;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.factory.ComponentFactory;
import com.orwellg.yggdrasil.net.client.producer.CommandProducerConfig;
import com.orwellg.yggdrasil.net.client.producer.GeneratorIdCommandProducer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.apache.kafka.common.utils.Time;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.util.*;

public class GenerateProcessedMessageBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(GenerateProcessedMessageBolt.class);

    private GeneratorIdCommandProducer idGeneratorClient;

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_NAME, Fields.RESULT));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        initializeIdGeneratorClient();
    }

    private void initializeIdGeneratorClient() {
        Properties props = new Properties();
        props.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.PLAINTEXT.name());
        props.setProperty(CommandProducerConfig.BOOTSTRAP_SERVERS_CONFIG, ComponentFactory.getConfigurationParams().getZookeeperConnection());
        props.setProperty(CommandProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty(CommandProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        idGeneratorClient = new GeneratorIdCommandProducer(new CommandProducerConfig(props), 1, Time.SYSTEM);
    }

    @Override
    public void execute(Tuple input) {
        String key = input.getStringByField(Fields.KEY);
        String processId = input.getStringByField(Fields.PROCESS_ID);
        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        LOG.info("{} Generating processed message", logPrefix);

        try {
            TransactionInfo event = (TransactionInfo) input.getValueByField(Fields.EVENT_DATA);
            TransactionEarmark earmark = (TransactionEarmark) input.getValueByField(Fields.EARMARK);
            List<CardTransaction> transactionList = (List<CardTransaction>) input.getValueByField(Fields.TRANSACTION_LIST);
            String responseKey = idGeneratorClient.getGeneralUniqueId();

            if (earmark == null) {
                throw new IllegalArgumentException("No earmark information found - cannot process authorisation reversal");
            }
            if (event.getSettlementAmount().compareTo(earmark.getAmount().abs()) > 0) {
                throw new IllegalArgumentException("Authorisation reversal amount is greater than earmarked amount");
            }
            if (transactionList == null || transactionList.isEmpty()){
                throw new IllegalArgumentException("Empty transaction list - cannot process authorisation reversal");
            }
            CardTransaction lastTransaction = transactionList.get(0);
            if (event.getSettlementAmount().compareTo(lastTransaction.getWirecardAmount().abs()) < 0) {
                throw new IllegalArgumentException("Authorisation reversal amount is greater than amount sent to Wirecard");
            }

            GpsMessageProcessed processedMessage = generateMessageProcessed(event, earmark, lastTransaction, logPrefix);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_NAME, CardTransactionEvents.RESPONSE_MESSAGE.getEventName());
            values.put(Fields.RESULT, processedMessage);
            send(input, values);
        } catch (Exception e) {
            LOG.error("{}Error generating processed message. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    private GpsMessageProcessed generateMessageProcessed(
            TransactionInfo transactionInfo, TransactionEarmark lastEarmark, CardTransaction lastTransaction,
            String logPrefix) {

        LOG.debug("{}Generating GPS message processed", logPrefix);

        GpsMessageProcessed gpsMessageProcessed = GpsMessageProcessedFactory.from(transactionInfo);

        gpsMessageProcessed.setEarmarkAmount(DecimalTypeUtils.toDecimal(transactionInfo.getSettlementAmount()));
        gpsMessageProcessed.setEarmarkCurrency(lastEarmark.getInternalAccountCurrency());
        gpsMessageProcessed.setWirecardAmount(DecimalTypeUtils.toDecimal(transactionInfo.getSettlementAmount().negate()));
        gpsMessageProcessed.setWirecardCurrency(transactionInfo.getSettlementCurrency());

        gpsMessageProcessed.setAppliedEarmarkAmount(DecimalTypeUtils.toDecimal(
                lastEarmark.getAmount().add(transactionInfo.getSettlementAmount())));
        gpsMessageProcessed.setAppliedEarmarkCurrency(lastEarmark.getInternalAccountCurrency());
        gpsMessageProcessed.setAppliedWirecardAmount(DecimalTypeUtils.toDecimal(
                lastTransaction.getWirecardAmount().subtract(transactionInfo.getSettlementAmount())));
        gpsMessageProcessed.setAppliedWirecardCurrency(transactionInfo.getSettlementCurrency());

        gpsMessageProcessed.setInternalAccountCurrency(lastEarmark.getInternalAccountCurrency());
        gpsMessageProcessed.setInternalAccountId(lastEarmark.getInternalAccountId());

        LOG.debug("{}GPS message processed generated: {}", logPrefix, gpsMessageProcessed);
        return gpsMessageProcessed;
    }
}
