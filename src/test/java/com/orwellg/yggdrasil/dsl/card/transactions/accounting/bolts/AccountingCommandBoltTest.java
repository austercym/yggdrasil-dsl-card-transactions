package com.orwellg.yggdrasil.dsl.card.transactions.accounting.bolts;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.avro.types.command.accounting.AccountingCommandData;
import com.orwellg.umbrella.avro.types.command.accounting.BalanceUpdateType;
import com.orwellg.umbrella.avro.types.command.accounting.TransactionDirection;
import com.orwellg.umbrella.avro.types.commons.TransactionType;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.umbrella.commons.utils.enums.Systems;
import com.orwellg.yggdrasil.commons.net.Cluster;
import com.orwellg.yggdrasil.commons.net.Node;
import com.orwellg.yggdrasil.commons.utils.enums.SpecialAccountTypes;
import com.orwellg.yggdrasil.dsl.card.transactions.accounting.AccountingTopology;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class AccountingCommandBoltTest {

    private AccountingCommandBolt bolt;

    @Before
    public void setUp() {
        bolt = new TestableAccountingCommandBolt();
        bolt.prepare(null, null, null);
        bolt.declareFieldsDefinition();
    }

    @Test
    public void executeWhenNoAccountingRequiredShouldMoveProcessingToDifferentStream() {
        // arrange
        MessageProcessed processed = new MessageProcessed();
        processed.setEarmarkAmount(DecimalTypeUtils.toDecimal(19.09));
        processed.setClientAmount(DecimalTypeUtils.toDecimal(0));
        processed.setWirecardAmount(DecimalTypeUtils.toDecimal(0));

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(processed);

        OutputCollector collector = mock(OutputCollector.class);
        bolt.setCollector(collector);

        // act
        bolt.execute(input);

        // assert
        verify(collector).emit(eq(AccountingTopology.NO_ACCOUNTING_STREAM), any(Tuple.class), any());
    }

    @Test
    public void executeWhenClientDebitShouldCreateAccountingCommand() {
        // arrange
        MessageProcessed processed = new MessageProcessed();
        processed.setClientAmount(DecimalTypeUtils.toDecimal(-19.09));
        processed.setClientCurrency("bar");
        processed.setWirecardAmount(DecimalTypeUtils.toDecimal(19.09));
        processed.setWirecardCurrency("bar");
        processed.setInternalAccountId("42");

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(processed);

        OutputCollector collector = mock(OutputCollector.class);
        bolt.setCollector(collector);

        Node node = mock(Node.class);
        when(node.getSpecialAccount(SpecialAccountTypes.GPS.getLiteral())).thenReturn("foo");

        Cluster cluster = mock(Cluster.class);
        bolt.setProcessorCluster(cluster);
        when(cluster.nodeByAccount(any(String.class))).thenReturn(node);

        // act
        bolt.execute(input);

        // assert
        verify(collector).emit(
                any(Tuple.class),
                argThat(result -> result.stream()
                        .filter(AccountingCommandData.class::isInstance)
                        .map(AccountingCommandData.class::cast)
                        .anyMatch(item -> item.getAccountingInfo() != null
                                && item.getAccountingInfo().getDebitAccount() != null
                                && "42".equals(item.getAccountingInfo().getDebitAccount().getAccountId())
                                && BalanceUpdateType.ALL.equals(item.getAccountingInfo().getDebitBalanceUpdate())
                                && item.getAccountingInfo().getCreditAccount() != null
                                && "foo".equals(item.getAccountingInfo().getCreditAccount().getAccountId())
                                && BalanceUpdateType.ALL.equals(item.getAccountingInfo().getCreditBalanceUpdate())
                                && item.getTransactionInfo() != null
                                && item.getTransactionInfo().getAmount().getValue().compareTo(BigDecimal.valueOf(19.09)) == 0
                                && "bar".equals(item.getTransactionInfo().getCurrency())
                                && Systems.CARDS_GPS.getSystem().equals(item.getTransactionInfo().getSystem())
                                && TransactionDirection.INTERNAL.equals(item.getTransactionInfo().getDirection())
                                && TransactionType.DEBIT.equals(item.getTransactionInfo().getTransactionType())
                        )));
    }

    @Test
    public void executeWhenClientCreditShouldCreateAccountingCommand() {
        // arrange
        MessageProcessed processed = new MessageProcessed();
        processed.setClientAmount(DecimalTypeUtils.toDecimal(19.09));
        processed.setClientCurrency("bar");
        processed.setWirecardAmount(DecimalTypeUtils.toDecimal(-19.09));
        processed.setWirecardCurrency("bar");
        processed.setInternalAccountId("42");

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(processed);

        OutputCollector collector = mock(OutputCollector.class);
        bolt.setCollector(collector);

        Node node = mock(Node.class);
        when(node.getSpecialAccount(SpecialAccountTypes.GPS.getLiteral())).thenReturn("foo");

        Cluster cluster = mock(Cluster.class);
        bolt.setProcessorCluster(cluster);
        when(cluster.nodeByAccount(any(String.class))).thenReturn(node);

        // act
        bolt.execute(input);

        // assert
        verify(collector).emit(
                any(Tuple.class),
                argThat(result -> result.stream()
                        .filter(AccountingCommandData.class::isInstance)
                        .map(AccountingCommandData.class::cast)
                        .anyMatch(item -> item.getAccountingInfo() != null
                                && item.getAccountingInfo().getCreditAccount() != null
                                && "42".equals(item.getAccountingInfo().getCreditAccount().getAccountId())
                                && BalanceUpdateType.ALL.equals(item.getAccountingInfo().getCreditBalanceUpdate())
                                && item.getAccountingInfo().getDebitAccount() != null
                                && "foo".equals(item.getAccountingInfo().getDebitAccount().getAccountId())
                                && BalanceUpdateType.ALL.equals(item.getAccountingInfo().getDebitBalanceUpdate())
                                && item.getTransactionInfo() != null
                                && item.getTransactionInfo().getAmount().getValue().compareTo(BigDecimal.valueOf(19.09)) == 0
                                && "bar".equals(item.getTransactionInfo().getCurrency())
                                && Systems.CARDS_GPS.getSystem().equals(item.getTransactionInfo().getSystem())
                                && TransactionDirection.INTERNAL.equals(item.getTransactionInfo().getDirection())
                                && TransactionType.CREDIT.equals(item.getTransactionInfo().getTransactionType())
                        )));
    }
}