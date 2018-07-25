package com.orwellg.yggdrasil.dsl.card.transactions.externallyauthorised.bolts;

import com.google.common.collect.Lists;
import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.avro.types.commons.Decimal;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.card.transaction.commons.bolts.Fields;
import com.orwellg.yggdrasil.card.transaction.commons.model.TransactionInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils.isEqual;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

public class GenerateProcessedMessageBoltTest {

    private GenerateProcessedMessageBolt bolt;

    private List<CardTransaction> createTransactionHistory(double earmarkAmount) {
        return IntStream.range(0, 1)
                .mapToObj(id -> {
                    CardTransaction historicalTransaction = new CardTransaction();
                    historicalTransaction.setClientAmount(BigDecimal.ZERO);
                    historicalTransaction.setWirecardAmount(BigDecimal.ZERO);
                    historicalTransaction.setEarmarkAmount(BigDecimal.valueOf(earmarkAmount));
                    historicalTransaction.setProviderMessageId(String.valueOf(id));
                    return historicalTransaction;
                })
                .collect(Collectors.toList());
    }

    private boolean isNullOrZero(Decimal amount) {
        return amount == null || amount.getValue().compareTo(BigDecimal.ZERO) == 0;
    }

    @Before
    public void setUp() {
        bolt = new GenerateProcessedMessageBolt();
        bolt.declareFieldsDefinition();
    }

    @Test
    public void executeWhenNotDeclinedExternallyShouldThrowException() {
        // arrange
        TransactionInfo transaction = new TransactionInfo();
        transaction.setSettlementAmount(BigDecimal.valueOf(-19.09));
        transaction.setSettlementCurrency("FOO");
        transaction.setProviderMessageId("42");
        Message message = new Message();
        message.setTxnType("A");
        message.setTxnStatCode("A");
        transaction.setMessage(message);

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(transaction);
        when(input.getValueByField(Fields.TRANSACTION_LIST)).thenReturn(createTransactionHistory(-20.15));

        OutputCollector collector = mock(OutputCollector.class);
        bolt.setCollector(collector);

        // act
        bolt.execute(input);

        // assert
        verify(collector).reportError(any());
        verify(collector).fail(any());
    }

    @Test
    public void executeWhenDifferentAmountsShouldThrowException() {
        // arrange
        TransactionInfo transaction = new TransactionInfo();
        transaction.setSettlementAmount(BigDecimal.valueOf(-19.09));
        transaction.setSettlementCurrency("FOO");
        transaction.setProviderMessageId("42");
        Message message = new Message();
        message.setTxnType("A");
        message.setTxnStatCode("I");
        transaction.setMessage(message);

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(transaction);
        when(input.getValueByField(Fields.TRANSACTION_LIST)).thenReturn(createTransactionHistory(-20.15));

        OutputCollector collector = mock(OutputCollector.class);
        bolt.setCollector(collector);

        // act
        bolt.execute(input);

        // assert
        verify(collector).reportError(any());
        verify(collector).fail(any());
    }

    @Test
    public void executeWhenFirstAuthorisationMessageShouldNotAffectBalances() {
        // arrange
        TransactionInfo transaction = new TransactionInfo();
        transaction.setSettlementAmount(BigDecimal.valueOf(-19.09));
        transaction.setSettlementCurrency("FOO");
        transaction.setProviderMessageId("42");
        Message message = new Message();
        message.setTxnType("A");
        message.setTxnStatCode("I");
        transaction.setMessage(message);

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(transaction);
        when(input.getValueByField(Fields.TRANSACTION_LIST)).thenReturn(createTransactionHistory(0));

        OutputCollector collector = mock(OutputCollector.class);
        bolt.setCollector(collector);

        // act
        bolt.execute(input);

        // assert
        verify(collector).emit(
                any(Tuple.class),
                argThat(result -> result.stream()
                        .filter(MessageProcessed.class::isInstance)
                        .map(MessageProcessed.class::cast)
                        .anyMatch(messageProcessed ->
                                isNullOrZero(messageProcessed.getClientAmount())
                                        &&
                                        isNullOrZero(messageProcessed.getEarmarkAmount())
                                        &&
                                        isNullOrZero(messageProcessed.getWirecardAmount())
                                        &&
                                        isNullOrZero(messageProcessed.getTotalClientAmount())
                                        &&
                                        isNullOrZero(messageProcessed.getTotalEarmarkAmount())
                                        &&
                                        isNullOrZero(messageProcessed.getTotalWirecardAmount()))));
    }

    @Test
    public void executeWhenFirstAuthorisationDeclinedMessageShouldNotAffectBalances() {
        // arrange
        TransactionInfo transaction = new TransactionInfo();
        transaction.setSettlementAmount(BigDecimal.valueOf(-19.09));
        transaction.setSettlementCurrency("FOO");
        transaction.setProviderMessageId("42");
        Message message = new Message();
        message.setTxnType("A");
        message.setTxnStatCode("I");
        transaction.setMessage(message);

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(transaction);
        when(input.getValueByField(Fields.TRANSACTION_LIST)).thenReturn(Lists.newArrayList());

        OutputCollector collector = mock(OutputCollector.class);
        bolt.setCollector(collector);

        // act
        bolt.execute(input);

        // assert
        verify(collector).emit(
                any(Tuple.class),
                argThat(result -> result.stream()
                        .filter(MessageProcessed.class::isInstance)
                        .map(MessageProcessed.class::cast)
                        .anyMatch(messageProcessed ->
                                isNullOrZero(messageProcessed.getClientAmount())
                                        &&
                                        isNullOrZero(messageProcessed.getEarmarkAmount())
                                        &&
                                        isNullOrZero(messageProcessed.getWirecardAmount())
                                        &&
                                        isNullOrZero(messageProcessed.getTotalClientAmount())
                                        &&
                                        isNullOrZero(messageProcessed.getTotalEarmarkAmount())
                                        &&
                                        isNullOrZero(messageProcessed.getTotalWirecardAmount()))));
    }

    @Test
    public void executeWhenTimeoutShouldRevertBalanceChanges() {
        // arrange
        TransactionInfo transaction = new TransactionInfo();
        transaction.setSettlementAmount(BigDecimal.valueOf(-19.09));
        transaction.setSettlementCurrency("FOO");
        transaction.setProviderMessageId("42");
        Message message = new Message();
        message.setTxnType("A");
        message.setTxnStatCode("I");
        transaction.setMessage(message);

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(transaction);
        when(input.getValueByField(Fields.TRANSACTION_LIST)).thenReturn(createTransactionHistory(-19.09));

        OutputCollector collector = mock(OutputCollector.class);
        bolt.setCollector(collector);

        // act
        bolt.execute(input);

        // assert
        verify(collector).emit(
                any(Tuple.class),
                argThat(result -> result.stream()
                        .filter(MessageProcessed.class::isInstance)
                        .map(MessageProcessed.class::cast)
                        .anyMatch(messageProcessed ->
                                isNullOrZero(messageProcessed.getClientAmount())
                                        &&
                                        isEqual(messageProcessed.getEarmarkAmount(), 19.09)
                                        &&
                                        isNullOrZero(messageProcessed.getWirecardAmount())
                                        &&
                                        isEqual(messageProcessed.getTotalClientAmount(), 0)
                                        &&
                                        isEqual(messageProcessed.getTotalEarmarkAmount(), 0)
                                        &&
                                        isEqual(messageProcessed.getTotalWirecardAmount(), 0))));
    }
}