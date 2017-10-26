package com.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class CardPresenceResolverTest {

    @Parameterized.Parameters(name = "{index}: de61={0} expectedIsCardPresent={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[]{"1010010000500826T3W 1NL", true},
                new Object[]{"1025108006600372DUBLIN 2", false},
                new Object[]{"\n\t\t\t", false}
        );
    }

    private String de61;

    private Boolean expectedIsCardPresent;

    private CardPresenceResolver cardPresenceResolver;

    public CardPresenceResolverTest(String de61, Boolean expectedIsCardPresent) {
        this.de61 = de61;
        this.expectedIsCardPresent = expectedIsCardPresent;
    }

    @Before
    public void initialize() {
        cardPresenceResolver = new CardPresenceResolver();
    }

    @Test
    public void getTypeShouldReturnCorrectValue() {
        // arrange
        Message message = new Message();
        message.setPOSDataDE61(de61);

        // act
        Boolean result = cardPresenceResolver.isCardPresent(message);

        // assert
        assertNotNull(result);
        assertEquals(expectedIsCardPresent, result);
    }
}
