package com.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@RunWith(Parameterized.class)
public class StatusValidationServiceImplTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { CardStatus.ACTIVE, true },
                { CardStatus.INACTIVE, false },
                { CardStatus.BLOCKED, false },
                { CardStatus.CANCELLED, false },
                { CardStatus.EXPIRED, false }
        });
    }

    private CardStatus status;

    private Boolean expectedIsValid;

    private StatusValidationServiceImpl validator;

    public StatusValidationServiceImplTest(CardStatus status, Boolean expectedIsValid) {
        this.status = status;
        this.expectedIsValid = expectedIsValid;
    }

    @Before
    public void initialize() {
        validator = new StatusValidationServiceImpl();
    }

    @Test
    public void validateShouldReturnCorrectIsValidFlag(){
        // arrange
        CardSettings settings = new CardSettings();
        settings.setStatus(status);

        // act
        ValidationResult result = validator.validate(null, settings);

        // assert
        assertNotNull(result);
        assertEquals(expectedIsValid, result.getIsValid());
        if (!expectedIsValid)
        {
            assertNotNull(result.getMessage());
            assertThat(result.getMessage().toLowerCase(), containsString(status.toString().toLowerCase()));
        }
    }
}