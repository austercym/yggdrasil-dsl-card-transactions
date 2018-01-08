package com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardStatus;
import com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services.StatusValidator;
import com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services.ValidationResult;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

@RunWith(Parameterized.class)
public class StatusValidatorTest {

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

    private StatusValidator validator;

    public StatusValidatorTest(CardStatus status, Boolean expectedIsValid) {
        this.status = status;
        this.expectedIsValid = expectedIsValid;
    }

    @Before
    public void initialize() {
        validator = new StatusValidator();
    }

    @Test
    public void validateShouldReturnCorrectIsValidFlag(){
        // arrange
        CardSettings settings = new CardSettings();
        settings.setStatus(status);

        // act
        ValidationResult result = validator.validate(null, settings);

        // assert
        Assert.assertNotNull(result);
        Assert.assertEquals(expectedIsValid, result.getIsValid());
        if (!expectedIsValid)
        {
            Assert.assertNotNull(result.getMessage());
            Assert.assertThat(result.getMessage().toLowerCase(), CoreMatchers.containsString(status.toString().toLowerCase()));
        }
    }
}
