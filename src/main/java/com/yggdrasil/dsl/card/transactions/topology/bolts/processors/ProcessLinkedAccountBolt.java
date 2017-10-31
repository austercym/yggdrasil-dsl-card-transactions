package com.yggdrasil.dsl.card.transactions.topology.bolts.processors;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.LinkedAccountHistory;
import org.apache.storm.tuple.Tuple;

import java.util.Arrays;
import java.util.List;

public class ProcessLinkedAccountBolt extends BasicRichBolt {

    @Override
    public void declareFieldsDefinition() {
        addFielsDefinition(Arrays.asList("key", "processId", "eventData", "message"));
    }

    @Override
    public void execute(Tuple tuple) {

        List<Object> inputValues = tuple.getValues();
        String key = (String) inputValues.get(0);
        String originalProcessId = (String)inputValues.get(1);
        Message eventData = (Message) inputValues.get(2);
        List<LinkedAccountHistory> linkdAccounts = (List<LinkedAccountHistory>) inputValues.get(3);

        //1. if we have more than 1 account - throw an error


        //2. update Message object with account id data
        

    }
}
