package com.orwellg.yggdrasil.dsl.card.transactions.common.bolts;

import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;

public class GenericMessageProcessingBoltTest {

    private GenericMessageProcessingBolt bolt;

    @Before
    public void setUp() {
        bolt = new GenericMessageProcessingBolt();
        bolt.declareFieldsDefinition();
    }

    @Test
    public void executeWhenNegativeSettlementAmountShouldReturnValidAmounts() {
        // arrange
        TransactionInfo transaction = new TransactionInfo();
        transaction.setSettlementAmount(BigDecimal.valueOf(-19.09));
        transaction.setSettlementCurrency("FOO");
        transaction.setMessage(new Message());

        CardTransaction previousTransaction = new CardTransaction();
        previousTransaction.setWirecardAmount(BigDecimal.valueOf(20.15));
        previousTransaction.setClientAmount(BigDecimal.valueOf(-20.15));
        previousTransaction.setInternalAccountCurrency("BAR");
        List<CardTransaction> transactionList = Collections.singletonList(previousTransaction);

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(transaction);
        when(input.getValueByField(Fields.TRANSACTION_LIST)).thenReturn(transactionList);

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
                        .anyMatch(item -> item.getWirecardAmount() != null
                                &&
                                item.getWirecardAmount().getValue().compareTo(BigDecimal.valueOf(19.09)) == 0
                                &&
                                "FOO".equals(item.getWirecardCurrency())
                                &&
                                item.getClientAmount() != null
                                &&
                                item.getClientAmount().getValue().compareTo(BigDecimal.valueOf(-19.09)) == 0
                                &&
                                "BAR".equals(item.getClientCurrency())
                                &&
                                item.getTotalWirecardAmount() != null
                                &&
                                item.getTotalWirecardAmount().getValue().compareTo(BigDecimal.valueOf(39.24)) == 0
                                &&
                                "FOO".equals(item.getTotalWirecardCurrency())
                                &&
                                item.getTotalClientAmount() != null
                                &&
                                item.getTotalClientAmount().getValue().compareTo(BigDecimal.valueOf(-39.24)) == 0
                                &&
                                "BAR".equals(item.getTotalClientCurrency())
                                &&
                                item.getTotalEarmarkAmount() != null
                                &&
                                item.getTotalEarmarkAmount().getValue().compareTo(BigDecimal.ZERO) == 0
                                &&
                                "BAR".equals(item.getTotalEarmarkCurrency())
                                &&
                                item.getTotalFeesAmount() != null
                                &&
                                item.getTotalFeesAmount().getValue().compareTo(BigDecimal.ZERO) == 0
                                &&
                                "BAR".equals(item.getTotalFeesCurrency()))));
    }

    @Test
    public void executeWhenReversedAmountSmallerThenOriginalAmountShouldReturnValidAmounts() {
        // arrange
        TransactionInfo transaction = new TransactionInfo();
        transaction.setSettlementAmount(BigDecimal.valueOf(19.09));
        transaction.setSettlementCurrency("FOO");
        transaction.setMessage(new Message());

        CardTransaction previousTransaction = new CardTransaction();
        previousTransaction.setWirecardAmount(BigDecimal.valueOf(20.15));
        previousTransaction.setClientAmount(BigDecimal.valueOf(-20.15));
        previousTransaction.setInternalAccountCurrency("BAR");
        List<CardTransaction> transactionList = Collections.singletonList(previousTransaction);

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(transaction);
        when(input.getValueByField(Fields.TRANSACTION_LIST)).thenReturn(transactionList);

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
                        .anyMatch(item -> item.getWirecardAmount() != null
                                &&
                                item.getWirecardAmount().getValue().compareTo(BigDecimal.valueOf(-19.09)) == 0
                                &&
                                "FOO".equals(item.getWirecardCurrency())
                                &&
                                item.getClientAmount() != null
                                &&
                                item.getClientAmount().getValue().compareTo(BigDecimal.valueOf(19.09)) == 0
                                &&
                                "BAR".equals(item.getClientCurrency())
                                &&
                                item.getTotalWirecardAmount() != null
                                &&
                                item.getTotalWirecardAmount().getValue().compareTo(BigDecimal.valueOf(1.06)) == 0
                                &&
                                "FOO".equals(item.getTotalWirecardCurrency())
                                &&
                                item.getTotalClientAmount() != null
                                &&
                                item.getTotalClientAmount().getValue().compareTo(BigDecimal.valueOf(-1.06)) == 0
                                &&
                                "BAR".equals(item.getTotalClientCurrency())
                                &&
                                item.getTotalEarmarkAmount() != null
                                &&
                                item.getTotalEarmarkAmount().getValue().compareTo(BigDecimal.ZERO) == 0
                                &&
                                "BAR".equals(item.getTotalEarmarkCurrency())
                                &&
                                item.getTotalFeesAmount() != null
                                &&
                                item.getTotalFeesAmount().getValue().compareTo(BigDecimal.ZERO) == 0
                                &&
                                "BAR".equals(item.getTotalFeesCurrency()))));
    }

    @Test
    public void executeWhenReversedAmountEqualOriginalAmountShouldReturnValidAmounts() {
        // arrange
        TransactionInfo transaction = new TransactionInfo();
        transaction.setSettlementAmount(BigDecimal.valueOf(19.09));
        transaction.setSettlementCurrency("FOO");
        transaction.setMessage(new Message());

        CardTransaction previousTransaction = new CardTransaction();
        previousTransaction.setWirecardAmount(BigDecimal.valueOf(19.09));
        previousTransaction.setClientAmount(BigDecimal.valueOf(-19.09));
        previousTransaction.setInternalAccountCurrency("BAR");
        List<CardTransaction> transactionList = Collections.singletonList(previousTransaction);

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(transaction);
        when(input.getValueByField(Fields.TRANSACTION_LIST)).thenReturn(transactionList);

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
                        .anyMatch(item -> item.getWirecardAmount() != null
                                &&
                                item.getWirecardAmount().getValue().compareTo(BigDecimal.valueOf(-19.09)) == 0
                                &&
                                "FOO".equals(item.getWirecardCurrency())
                                &&
                                item.getClientAmount() != null
                                &&
                                item.getClientAmount().getValue().compareTo(BigDecimal.valueOf(19.09)) == 0
                                &&
                                "BAR".equals(item.getClientCurrency())
                                &&
                                item.getTotalWirecardAmount() != null
                                &&
                                item.getTotalWirecardAmount().getValue().compareTo(BigDecimal.ZERO) == 0
                                &&
                                "FOO".equals(item.getTotalWirecardCurrency())
                                &&
                                item.getTotalClientAmount() != null
                                &&
                                item.getTotalClientAmount().getValue().compareTo(BigDecimal.ZERO) == 0
                                &&
                                "BAR".equals(item.getTotalClientCurrency())
                                &&
                                item.getTotalEarmarkAmount() != null
                                &&
                                item.getTotalEarmarkAmount().getValue().compareTo(BigDecimal.ZERO) == 0
                                &&
                                "BAR".equals(item.getTotalEarmarkCurrency())
                                &&
                                item.getTotalFeesAmount() != null
                                &&
                                item.getTotalFeesAmount().getValue().compareTo(BigDecimal.ZERO) == 0
                                &&
                                "BAR".equals(item.getTotalFeesCurrency()))));
    }

    @Test
    public void executeWhenReversedAmountGreaterThenOriginalAmountShouldReturnValidAmounts() {
        // arrange
        TransactionInfo transaction = new TransactionInfo();
        transaction.setSettlementAmount(BigDecimal.valueOf(19.09));
        transaction.setSettlementCurrency("FOO");
        transaction.setMessage(new Message());

        CardTransaction previousTransaction = new CardTransaction();
        previousTransaction.setWirecardAmount(BigDecimal.valueOf(3.08));
        previousTransaction.setClientAmount(BigDecimal.valueOf(-3.08));
        previousTransaction.setInternalAccountCurrency("BAR");
        List<CardTransaction> transactionList = Collections.singletonList(previousTransaction);

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(transaction);
        when(input.getValueByField(Fields.TRANSACTION_LIST)).thenReturn(transactionList);

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
                        .anyMatch(item -> item.getWirecardAmount() != null
                                &&
                                item.getWirecardAmount().getValue().compareTo(BigDecimal.valueOf(-19.09)) == 0
                                &&
                                "FOO".equals(item.getWirecardCurrency())
                                &&
                                item.getClientAmount() != null
                                &&
                                item.getClientAmount().getValue().compareTo(BigDecimal.valueOf(19.09)) == 0
                                &&
                                "BAR".equals(item.getClientCurrency())
                                &&
                                item.getTotalWirecardAmount() != null
                                &&
                                item.getTotalWirecardAmount().getValue().compareTo(BigDecimal.valueOf(-16.01)) == 0
                                &&
                                "FOO".equals(item.getTotalWirecardCurrency())
                                &&
                                item.getTotalClientAmount() != null
                                &&
                                item.getTotalClientAmount().getValue().compareTo(BigDecimal.valueOf(16.01)) == 0
                                &&
                                "BAR".equals(item.getTotalClientCurrency())
                                &&
                                item.getTotalEarmarkAmount() != null
                                &&
                                item.getTotalEarmarkAmount().getValue().compareTo(BigDecimal.ZERO) == 0
                                &&
                                "BAR".equals(item.getTotalEarmarkCurrency())
                                &&
                                item.getTotalFeesAmount() != null
                                &&
                                item.getTotalFeesAmount().getValue().compareTo(BigDecimal.ZERO) == 0
                                &&
                                "BAR".equals(item.getTotalFeesCurrency()))));
    }

    @Test
    public void executeWhenNoPreviousTransactionShouldThrowException() {
        // arrange
        TransactionInfo transaction = new TransactionInfo();
        transaction.setSettlementAmount(BigDecimal.valueOf(19.09));
        transaction.setSettlementCurrency("FOO");
        transaction.setMessage(new Message());

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(transaction);
        when(input.getValueByField(Fields.TRANSACTION_LIST)).thenReturn(Collections.<CardTransaction>emptyList());

        OutputCollector collector = mock(OutputCollector.class);
        bolt.setCollector(collector);

        // act
        bolt.execute(input);

        // assert
        verify(collector).reportError(any(IllegalArgumentException.class));
    }
}