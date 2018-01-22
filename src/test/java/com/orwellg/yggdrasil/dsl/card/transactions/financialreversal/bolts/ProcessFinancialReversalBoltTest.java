package com.orwellg.yggdrasil.dsl.card.transactions.financialreversal.bolts;

import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

import static org.mockito.Mockito.*;

public class ProcessFinancialReversalBoltTest {

    private ProcessFinancialReversalBolt bolt;

    @Before
    public void setUp() {
        bolt = new ProcessFinancialReversalBolt();
        bolt.declareFieldsDefinition();
    }

    @Test
    public void executeWhenReversedAmountEqualOriginalAmount() {
        // arrange
        TransactionInfo transaction = new TransactionInfo();
        transaction.setSettlementAmount(BigDecimal.valueOf(19.09));
        transaction.setSettlementCurrency("FOO");
        transaction.setMessage(new Message());

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(transaction);

        OutputCollector collector = mock(OutputCollector.class);
        bolt.setCollector(collector);

        // act
        bolt.execute(input);

        // assert
        verify(collector).emit(
                any(Tuple.class),
                argThat(result -> result.stream()
                        .filter(GpsMessageProcessed.class::isInstance)
                        .map(GpsMessageProcessed.class::cast)
                        .anyMatch(item ->
                                item.getWirecardAmount().getValue().compareTo(BigDecimal.valueOf(-19.09)) == 0
                                &&
                                "FOO".equals(item.getWirecardCurrency())
                                &&
                                item.getBlockedClientAmount().getValue().compareTo(BigDecimal.valueOf(-19.09)) == 0
                                &&
                                "FOO".equals(item.getBlockedClientCurrency()))));
    }
}