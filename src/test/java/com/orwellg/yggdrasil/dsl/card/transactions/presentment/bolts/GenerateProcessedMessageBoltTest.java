package com.orwellg.yggdrasil.dsl.card.transactions.presentment.bolts;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.LinkedAccount;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

import static com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils.isEqual;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

public class GenerateProcessedMessageBoltTest {

    private GenerateProcessedMessageBolt bolt;

    @Before
    public void setUp() {
        bolt = new GenerateProcessedMessageBolt();
        bolt.declareFieldsDefinition();
    }

    @Test
    public void executeWhenOfflinePresentmentShouldCalculateAmounts() {
        // arrange
        Message message = new Message();
        message.setTxnType("P");
        TransactionInfo transaction = new TransactionInfo();
        transaction.setMessage(message);
        transaction.setSettlementAmount(BigDecimal.valueOf(-19.09));
        transaction.setSettlementCurrency("bar");
        LinkedAccount linkedAccount = new LinkedAccount();
        linkedAccount.setInternalAccountId("42");
        linkedAccount.setInternalAccountCurrency("foo");

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(transaction);
        when(input.contains(Fields.LINKED_ACCOUNT)).thenReturn(true);
        when(input.getValueByField(Fields.LINKED_ACCOUNT)).thenReturn(linkedAccount);

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
                        .anyMatch(item -> isEqual(item.getEarmarkAmount(), 0)
                                && "foo".equals(item.getEarmarkCurrency())
                                && isEqual(item.getTotalEarmarkAmount(), 0)
                                && "foo".equals(item.getTotalEarmarkCurrency())
                                && isEqual(item.getClientAmount(), -19.09)
                                && "foo".equals(item.getClientCurrency())
                                && isEqual(item.getTotalClientAmount(), -19.09)
                                && "foo".equals(item.getTotalClientCurrency())
                                && isEqual(item.getWirecardAmount(), 19.09)
                                && "bar".equals(item.getWirecardCurrency())
                                && isEqual(item.getTotalWirecardAmount(), 19.09)
                                && "bar".equals(item.getTotalWirecardCurrency())
                                && "42".equals(item.getInternalAccountId())
                                && "foo".equals(item.getInternalAccountCurrency())
                        )));
    }

    @Test
    public void executeWhenDifferentAmountsOnPresentmentAndAuthorisationShouldCalculateAmounts() {
        // arrange
        Message message = new Message();
        message.setTxnType("P");
        TransactionInfo transaction = new TransactionInfo();
        transaction.setMessage(message);
        transaction.setSettlementAmount(BigDecimal.valueOf(-19.09));
        transaction.setSettlementCurrency("bar");
        CardTransaction authorisation = new CardTransaction();
        authorisation.setEarmarkAmount(BigDecimal.valueOf(-20.15));
        authorisation.setClientAmount(BigDecimal.ZERO);
        authorisation.setWirecardAmount(BigDecimal.ZERO);
        authorisation.setInternalAccountId("42");
        authorisation.setInternalAccountCurrency("foo");

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(transaction);
        when(input.contains(Fields.LAST_TRANSACTION)).thenReturn(true);
        when(input.getValueByField(Fields.LAST_TRANSACTION)).thenReturn(authorisation);

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
                        .anyMatch(item -> isEqual(item.getEarmarkAmount(), 20.15)
                                && "foo".equals(item.getEarmarkCurrency())
                                && isEqual(item.getTotalEarmarkAmount(), 0)
                                && "foo".equals(item.getTotalEarmarkCurrency())
                                && isEqual(item.getClientAmount(), -19.09)
                                && "foo".equals(item.getClientCurrency())
                                && isEqual(item.getTotalClientAmount(), -19.09)
                                && "foo".equals(item.getTotalClientCurrency())
                                && isEqual(item.getWirecardAmount(), 19.09)
                                && "bar".equals(item.getWirecardCurrency())
                                && isEqual(item.getTotalWirecardAmount(), 19.09)
                                && "bar".equals(item.getTotalWirecardCurrency())
                                && "42".equals(item.getInternalAccountId())
                                && "foo".equals(item.getInternalAccountCurrency())
                        )));
    }

    @Test
    public void executeWhenSecondDebitPresentmentShouldCalculateAmounts() {
        // arrange
        Message message = new Message();
        message.setTxnType("P");
        TransactionInfo transaction = new TransactionInfo();
        transaction.setMessage(message);
        transaction.setSettlementAmount(BigDecimal.valueOf(-19.09));
        transaction.setSettlementCurrency("bar");
        CardTransaction firstPresentment = new CardTransaction();
        firstPresentment.setEarmarkAmount(BigDecimal.ZERO);
        firstPresentment.setClientAmount(BigDecimal.valueOf(-20.15));
        firstPresentment.setWirecardAmount(BigDecimal.valueOf(20.15));
        firstPresentment.setInternalAccountId("42");
        firstPresentment.setInternalAccountCurrency("foo");

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(transaction);
        when(input.contains(Fields.LAST_TRANSACTION)).thenReturn(true);
        when(input.getValueByField(Fields.LAST_TRANSACTION)).thenReturn(firstPresentment);

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
                        .anyMatch(item -> isEqual(item.getEarmarkAmount(), 0)
                                && "foo".equals(item.getEarmarkCurrency())
                                && isEqual(item.getTotalEarmarkAmount(), 0)
                                && "foo".equals(item.getTotalEarmarkCurrency())
                                && isEqual(item.getClientAmount(), -19.09)
                                && "foo".equals(item.getClientCurrency())
                                && isEqual(item.getTotalClientAmount(), -39.24)
                                && "foo".equals(item.getTotalClientCurrency())
                                && isEqual(item.getWirecardAmount(), 19.09)
                                && "bar".equals(item.getWirecardCurrency())
                                && isEqual(item.getTotalWirecardAmount(), 39.24)
                                && "bar".equals(item.getTotalWirecardCurrency())
                                && "42".equals(item.getInternalAccountId())
                                && "foo".equals(item.getInternalAccountCurrency())
                        )));
    }

    @Test
    public void executeWhenCreditPresentmentAfterDebitOneShouldCalculateAmounts() {
        // arrange
        Message message = new Message();
        message.setTxnType("P");
        TransactionInfo transaction = new TransactionInfo();
        transaction.setMessage(message);
        transaction.setSettlementAmount(BigDecimal.valueOf(19.09));
        transaction.setSettlementCurrency("bar");
        CardTransaction firstPresentment = new CardTransaction();
        firstPresentment.setEarmarkAmount(BigDecimal.ZERO);
        firstPresentment.setClientAmount(BigDecimal.valueOf(-20.15));
        firstPresentment.setWirecardAmount(BigDecimal.valueOf(20.15));
        firstPresentment.setInternalAccountId("42");
        firstPresentment.setInternalAccountCurrency("foo");

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(transaction);
        when(input.contains(Fields.LAST_TRANSACTION)).thenReturn(true);
        when(input.getValueByField(Fields.LAST_TRANSACTION)).thenReturn(firstPresentment);

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
                        .anyMatch(item -> isEqual(item.getEarmarkAmount(), 0)
                                && "foo".equals(item.getEarmarkCurrency())
                                && isEqual(item.getTotalEarmarkAmount(), 0)
                                && "foo".equals(item.getTotalEarmarkCurrency())
                                && isEqual(item.getClientAmount(), 19.09)
                                && "foo".equals(item.getClientCurrency())
                                && isEqual(item.getTotalClientAmount(), -1.06)
                                && "foo".equals(item.getTotalClientCurrency())
                                && isEqual(item.getWirecardAmount(), -19.09)
                                && "bar".equals(item.getWirecardCurrency())
                                && isEqual(item.getTotalWirecardAmount(), 1.06)
                                && "bar".equals(item.getTotalWirecardCurrency())
                                && "42".equals(item.getInternalAccountId())
                                && "foo".equals(item.getInternalAccountCurrency())
                        )));
    }
}