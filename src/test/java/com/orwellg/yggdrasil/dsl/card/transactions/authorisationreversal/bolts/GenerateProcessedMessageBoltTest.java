package com.orwellg.yggdrasil.dsl.card.transactions.authorisationreversal.bolts;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.avro.types.commons.Decimal;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.card.transaction.commons.DuplicateChecker;
import com.orwellg.yggdrasil.card.transaction.commons.model.TransactionInfo;
import com.orwellg.yggdrasil.dsl.card.transactions.common.bolts.Fields;
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

    @Before
    public void setUp() {
        bolt = new GenerateProcessedMessageBolt();
        bolt.declareFieldsDefinition();
    }

    private List<CardTransaction> createTransactionHistory(double earmarkAmount, int count) {
        return IntStream.range(0, count)
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

    @Test
    public void executeWhenDuplicatedMessageShouldNotAffectBalances() {
        // arrange
        TransactionInfo transaction = new TransactionInfo();
        transaction.setSettlementAmount(BigDecimal.valueOf(19.09));
        transaction.setSettlementCurrency("FOO");
        transaction.setProviderMessageId("42");
        Message message = new Message();
        message.setTxnType("D");
        transaction.setMessage(message);

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(transaction);
        when(input.getValueByField(Fields.TRANSACTION_LIST)).thenReturn(createTransactionHistory(-20.15, 2));

        OutputCollector collector = mock(OutputCollector.class);
        bolt.setCollector(collector);

        DuplicateChecker duplicateChecker = mock(DuplicateChecker.class);
        when(duplicateChecker.isDuplicate(any(), any())).thenReturn(true);
        bolt.setDuplicateChecker(duplicateChecker);

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
                                        isEqual(messageProcessed.getTotalClientAmount(), 0)
                                        &&
                                        isEqual(messageProcessed.getTotalEarmarkAmount(), -20.15)
                                        &&
                                        isEqual(messageProcessed.getTotalWirecardAmount(), 0))));
    }

    @Test
    public void executeWhenNotADuplicatedMessageShouldAffectEarmarkBalanceOnly() {
        // arrange
        TransactionInfo transaction = new TransactionInfo();
        transaction.setSettlementAmount(BigDecimal.valueOf(19.09));
        transaction.setSettlementCurrency("FOO");
        transaction.setProviderMessageId("42");
        Message message = new Message();
        message.setTxnType("D");
        transaction.setMessage(message);

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(transaction);
        when(input.getValueByField(Fields.TRANSACTION_LIST)).thenReturn(createTransactionHistory(-20.15, 2));

        OutputCollector collector = mock(OutputCollector.class);
        bolt.setCollector(collector);

        DuplicateChecker duplicateChecker = mock(DuplicateChecker.class);
        when(duplicateChecker.isDuplicate(any(), any())).thenReturn(false);
        bolt.setDuplicateChecker(duplicateChecker);

        // act
        bolt.execute(input);

        // assert
        verify(collector).emit(
                any(Tuple.class),
                argThat(result -> result.stream()
                        .filter(MessageProcessed.class::isInstance)
                        .map(MessageProcessed.class::cast)
                        .anyMatch(messageProcessed ->
                                (isNullOrZero(messageProcessed.getClientAmount()))
                                        &&
                                        messageProcessed.getEarmarkAmount().getValue().compareTo(BigDecimal.valueOf(19.09)) == 0
                                        &&
                                        isNullOrZero(messageProcessed.getWirecardAmount())
                                        &&
                                        isEqual(messageProcessed.getTotalClientAmount(), 0)
                                        &&
                                        isEqual(messageProcessed.getTotalEarmarkAmount(), -1.06)
                                        &&
                                        isEqual(messageProcessed.getTotalWirecardAmount(), 0))));
    }
}