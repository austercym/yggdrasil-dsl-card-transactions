package com.orwellg.yggdrasil.dsl.card.transactions.authorisationReversal.bolts;

import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionEarmark;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.umbrella.commons.types.utils.avro.RawMessageUtils;
import com.orwellg.umbrella.commons.utils.enums.CardTransactionEvents;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import com.orwellg.yggdrasil.dsl.card.transactions.utils.EventBuilder;
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

import java.math.BigDecimal;
import java.util.*;

public class GenerateProcessedMessageBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(GenerateProcessedMessageBolt.class);

    private GeneratorIdCommandProducer idGeneratorClient;

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.MESSAGE, Fields.TOPIC));
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
            BigDecimal newEarmarkAmount = earmark.getAmount().abs().subtract(event.getSettlementAmount().abs());
            if (newEarmarkAmount.compareTo(BigDecimal.ZERO) < 0) {
                throw new IllegalArgumentException("Authorisation reversal amount is greater than earmarked amount");
            }

            if (transactionList == null || transactionList.isEmpty()){
                throw new IllegalArgumentException("No transaction list found - cannot process authorisation reversal");
            }
            CardTransaction lastTransaction = transactionList.get(0);
            BigDecimal newWirecardAmount = lastTransaction.getWirecardAmount().abs().subtract(event.getSettlementAmount().abs());
            if (newWirecardAmount.compareTo(BigDecimal.ZERO) < 0) {
                throw new IllegalArgumentException("Authorisation reversal amount is greater than amount sent to Wirecard");
            }

            GpsMessageProcessed processedMessage = generateMessageProcessed(event, earmark, newEarmarkAmount, newWirecardAmount, logPrefix);

            Event responseEvent = new EventBuilder().buildResponseEvent(
                    this.getClass().getName(), CardTransactionEvents.RESPONSE_MESSAGE.getEventName(), processedMessage,
                    responseKey, key);

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.MESSAGE, RawMessageUtils.encodeToString(Event.SCHEMA$, responseEvent));
            // TODO: get response topic from input event or config
            values.put(Fields.TOPIC, "com.orwellg.gps.authorisation.reversal.response.1");
            send(input, values);
        } catch (Exception e) {
            LOG.error("{}Error generating processed message. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    private GpsMessageProcessed generateMessageProcessed(TransactionInfo transactionInfo, TransactionEarmark earmark, BigDecimal newEarmarkAmount, BigDecimal newWirecardAmount, String logPrefix) {

        LOG.debug("{}Generating gpsMessageProcessed message", logPrefix);

        GpsMessageProcessed gpsMessageProcessed = new GpsMessageProcessed();
        gpsMessageProcessed.setGpsMessageType(transactionInfo.getMessage().getTxnType());
        gpsMessageProcessed.setGpsTransactionLink(transactionInfo.getGpsTransactionLink());
        gpsMessageProcessed.setGpsTransactionId(transactionInfo.getGpsTransactionId());
        gpsMessageProcessed.setDebitCardId(transactionInfo.getDebitCardId());

        gpsMessageProcessed.setBlockedClientAmount(DecimalTypeUtils.toDecimal(newEarmarkAmount));
        gpsMessageProcessed.setBlockedClientCurrency(earmark.getInternalAccountCurrency());
        gpsMessageProcessed.setWirecardAmount(DecimalTypeUtils.toDecimal(newWirecardAmount));
        gpsMessageProcessed.setWirecardCurrency(transactionInfo.getSettlementCurrency());
        gpsMessageProcessed.setFeesAmount(DecimalTypeUtils.toDecimal(0));

        gpsMessageProcessed.setAppliedBlockedClientAmount(DecimalTypeUtils.toDecimal(earmark.getAmount()));
        gpsMessageProcessed.setAppliedBlockedClientCurrency(earmark.getInternalAccountCurrency());
        gpsMessageProcessed.setAppliedWirecardAmount(DecimalTypeUtils.toDecimal(transactionInfo.getSettlementAmount()));
        gpsMessageProcessed.setAppliedWirecardCurrency(transactionInfo.getSettlementCurrency());

        gpsMessageProcessed.setSpendGroup(transactionInfo.getSpendGroup());
        gpsMessageProcessed.setTransactionTimestamp(new Date().getTime());

        gpsMessageProcessed.setInternalAccountCurrency(earmark.getInternalAccountCurrency());
        gpsMessageProcessed.setInternalAccountId(earmark.getInternalAccountId());

        LOG.debug("{}Message generated: {}", logPrefix, gpsMessageProcessed);
        return gpsMessageProcessed;
    }
}
