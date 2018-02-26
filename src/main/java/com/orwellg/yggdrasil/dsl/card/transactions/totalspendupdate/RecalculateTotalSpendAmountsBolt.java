package com.orwellg.yggdrasil.dsl.card.transactions.totalspendupdate;

import com.orwellg.umbrella.avro.types.commons.Decimal;
import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.yggdrasil.dsl.card.transactions.services.TotalSpendAmountsCalculator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class RecalculateTotalSpendAmountsBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(RecalculateTotalSpendAmountsBolt.class);

    private TotalSpendAmountsCalculator calculator;

    void setCalculator(TotalSpendAmountsCalculator calculator) {
        this.calculator = calculator;
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        super.prepare(stormConf, context, collector);
        calculator = new TotalSpendAmountsCalculator();
    }

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList(
                Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA, Fields.NEW_TOTAL_SPEND_AMOUNTS));
    }

    @Override
    public void execute(Tuple input) {
        String key = input.getStringByField(Fields.KEY);
        String processId = input.getStringByField(Fields.PROCESS_ID);
        String logPrefix = String.format("[Key: %s][ProcessId: %s] ", key, processId);

        LOG.info("{}Recalculating total spend amounts", logPrefix, key, processId);

        try {
            GpsMessageProcessed eventData = (GpsMessageProcessed) input.getValueByField(Fields.EVENT_DATA);
            SpendingTotalAmounts spendAmounts = (SpendingTotalAmounts) input.getValueByField(Fields.TOTAL_AMOUNTS);
            CardTransaction authorisation = (CardTransaction) input.getValueByField(Fields.AUTHORISATION);
            SpendingTotalAmounts newSpendAmounts = null;

            if (isAcceptedAuthorisation(eventData) || isDebitPresentment(eventData)) {
                newSpendAmounts = calculator.recalculate(eventData, spendAmounts, authorisation);
            } else {
                LOG.info(
                        "{}No need for recalculation of spend total amounts - GpsMessageType={}, ResponseStatus={}",
                        logPrefix,
                        eventData == null ? null : eventData.getGpsMessageType(),
                        eventData == null || eventData.getEhiResponse() == null ? null : eventData.getEhiResponse().getResponsestatus());
            }

            Map<String, Object> values = new HashMap<>();
            values.put(Fields.KEY, key);
            values.put(Fields.PROCESS_ID, processId);
            values.put(Fields.EVENT_DATA, eventData);
            values.put(Fields.NEW_TOTAL_SPEND_AMOUNTS, newSpendAmounts);
            send(input, values);
        } catch (Exception e) {
            LOG.error("{}Error recalculating total spend amounts. Message: {},", logPrefix, e.getMessage(), e);
            error(e, input);
        }
    }

    private boolean isAcceptedAuthorisation(GpsMessageProcessed eventData) {
        return eventData != null && eventData.getEhiResponse() != null
                &&
                "A".equalsIgnoreCase(eventData.getGpsMessageType())
                &&
                "00".equals(eventData.getEhiResponse().getResponsestatus());
    }

    private boolean isDebitPresentment(GpsMessageProcessed eventData) {
        return eventData != null
                &&
                "P".equalsIgnoreCase(eventData.getGpsMessageType())
                && Optional.ofNullable(eventData.getClientAmount())
                        .map(Decimal::getValue)
                        .orElse(BigDecimal.ZERO)
                        .compareTo(BigDecimal.ZERO) < 0;
    }
}
