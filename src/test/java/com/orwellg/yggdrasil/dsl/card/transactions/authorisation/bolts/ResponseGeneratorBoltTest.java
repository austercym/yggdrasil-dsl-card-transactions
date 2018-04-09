package com.orwellg.yggdrasil.dsl.card.transactions.authorisation.bolts;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.AccountBalance;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.yggdrasil.card.transaction.commons.authorisation.services.AuthorisationResponseGenerator;
import com.orwellg.yggdrasil.card.transaction.commons.authorisation.services.ValidationResult;
import com.orwellg.yggdrasil.card.transaction.commons.model.TransactionInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

public class ResponseGeneratorBoltTest {

    private ResponseGeneratorBolt bolt;

    private AuthorisationResponseGenerator authorisationResponseGenerator = mock(AuthorisationResponseGenerator.class);

    @Before
    public void setUp() {
        bolt = new ResponseGeneratorBolt();
        bolt.declareFieldsDefinition();
        bolt.setAuthorisationResponseGenerator(authorisationResponseGenerator);
    }

    @Test
    public void executeShouldPassResponseFromGeneratorToOutputTuple() {
        // arrange
        TransactionInfo authorisation = new TransactionInfo();
        CardSettings cardSettings = new CardSettings();
        AccountBalance accountBalance = new AccountBalance();

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(authorisation);
        when(input.getValueByField(Fields.STATUS_VALIDATION_RESULT)).thenReturn(ValidationResult.valid());
        when(input.getValueByField(Fields.VELOCITY_LIMITS_VALIDATION_RESULT)).thenReturn(ValidationResult.valid());
        when(input.getValueByField(Fields.BALANCE_VALIDATION_RESULT)).thenReturn(ValidationResult.valid());
        when(input.getValueByField(Fields.MERCHANT_VALIDATION_RESULT)).thenReturn(ValidationResult.valid());
        when(input.getValueByField(Fields.TRANSACTION_TYPE_VALIDATION_RESULT)).thenReturn(ValidationResult.valid());
        when(input.getValueByField(Fields.CARD_SETTINGS)).thenReturn(cardSettings);
        when(input.getValueByField(Fields.ACCOUNT_BALANCE)).thenReturn(accountBalance);

        OutputCollector collector = mock(OutputCollector.class);
        bolt.setCollector(collector);

        MessageProcessed expectedResult = new MessageProcessed();
        when(authorisationResponseGenerator.getMessageProcessed(any(), any(), any(), any()))
                .thenReturn(expectedResult);

        // act
        bolt.execute(input);

        // assert
        verify(collector).emit(
                any(Tuple.class),
                argThat(result -> result.stream()
                        .filter(MessageProcessed.class::isInstance)
                        .map(MessageProcessed.class::cast)
                        .anyMatch(expectedResult::equals)));
    }
}