package com.yggdrasil.dsl.card.transactions.topology.bolts.event;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.tuple.Tuple;


import java.util.*;
import java.util.stream.Collectors;

public class ValidateAuthorisationBolt extends BasicRichBolt {

    private static final Logger LOG = LogManager.getLogger(SampleBolt.class);

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("status"));
    }

    @Override
    public void execute(Tuple tuple) {

        try{

            List<Object> inputValues = tuple.getValues();
            //todo: 0 is key
            String key = (String) inputValues.get(0);
            String processId = (String)inputValues.get(1);
            Message eventData = (Message) inputValues.get(2);
            List<CardTransaction> cardTransactions = (List<CardTransaction>) inputValues.get(3);

            //if (cardTransactions.size() > 0) {
            CardTransaction lastTransaction = cardTransactions.stream()
                        .filter(x -> x.getGpsTransactionId() != eventData.getTXnID())
                        .max(Comparator.comparing(CardTransaction::getTransactionTimestamp))
                        .get();

            if (lastTransaction != null){

            }
            else {
                //todo: offline presentment -> another stream
            }

            //}

            Map<String, Object> values = new HashMap<>();
            values.put("status", "processed");
            send(tuple, values);
        }catch (Exception e) {
            //todo: error
            LOG.error("The received event {} can not be decoded. Message: {}", tuple, e.getMessage(), e);
            error(e, tuple);
        }


    }
}
