package com.orwellg.yggdrasil.dsl.card.transactions.authorisation.bolts;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.accounting.AccountTransactionLog;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.ResponseCode;
import com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services.ValidationResult;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;

import static com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils.isEqual;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

public class ResponseGeneratorBoltTest {

    private ResponseGeneratorBolt bolt;

    @Before
    public void setUp() {
        bolt = new ResponseGeneratorBolt();
        bolt.declareFieldsDefinition();
    }

    @Test
    public void executeWhenAllValidationPassedShouldGenerateCorrectResponse() {
        // arrange
        Message message = new Message();
        message.setTxnType("A");
        TransactionInfo authorisation = new TransactionInfo();
        authorisation.setMessage(message);
        authorisation.setSettlementAmount(BigDecimal.valueOf(-19.09));
        authorisation.setSettlementCurrency("foo");
        authorisation.setIsBalanceEnquiry(false);
        CardSettings cardSettings = new CardSettings();
        cardSettings.setLinkedAccountId(42L);
        cardSettings.setLinkedAccountCurrency("foo");
        AccountTransactionLog accountTransactionLog = new AccountTransactionLog();
        accountTransactionLog.setActualBalance(BigDecimal.valueOf(100));
        accountTransactionLog.setLedgerBalance(BigDecimal.valueOf(200));

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(authorisation);
        when(input.getValueByField(Fields.STATUS_VALIDATION_RESULT)).thenReturn(ValidationResult.valid());
        when(input.getValueByField(Fields.VELOCITY_LIMITS_VALIDATION_RESULT)).thenReturn(ValidationResult.valid());
        when(input.getValueByField(Fields.BALANCE_VALIDATION_RESULT)).thenReturn(ValidationResult.valid());
        when(input.getValueByField(Fields.MERCHANT_VALIDATION_RESULT)).thenReturn(ValidationResult.valid());
        when(input.getValueByField(Fields.TRANSACTION_TYPE_VALIDATION_RESULT)).thenReturn(ValidationResult.valid());
        when(input.getValueByField(Fields.CARD_SETTINGS)).thenReturn(cardSettings);
        when(input.getValueByField(Fields.TRANSACTION_LOG)).thenReturn(accountTransactionLog);

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
                        .anyMatch(item -> isEqual(item.getEarmarkAmount(), -19.09)
                                && "foo".equals(item.getEarmarkCurrency())
                                && isEqual(item.getTotalEarmarkAmount(), -19.09)
                                && "foo".equals(item.getTotalEarmarkCurrency())
                                && isEqual(item.getClientAmount(), 0)
                                && "foo".equals(item.getClientCurrency())
                                && isEqual(item.getTotalClientAmount(), 0)
                                && "foo".equals(item.getTotalClientCurrency())
                                && isEqual(item.getWirecardAmount(), 0)
                                && "foo".equals(item.getWirecardCurrency())
                                && isEqual(item.getTotalWirecardAmount(), 0)
                                && "foo".equals(item.getTotalWirecardCurrency())
                                && item.getInternalAccountId() == 42
                                && "foo".equals(item.getInternalAccountCurrency())
                                && item.getEhiResponse() != null
                                && ResponseCode.ALL_GOOD.getCode().equals(item.getEhiResponse().getResponsestatus())
                                && Double.compare(item.getEhiResponse().getAvlBalance(), 80.91) == 0
                                && Double.compare(item.getEhiResponse().getCurBalance(), 200) == 0
                        )));
    }
}