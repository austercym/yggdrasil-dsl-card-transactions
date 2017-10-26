package com.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class TransactionTypeResolverTest {

    @Parameterized.Parameters(name = "{index}: expectedType={1}")
    public static Collection<Object[]> data() {

        ArrayList<Object[]> list = new ArrayList<>();
        Message message;

        message = new Message();
        message.setMCCCode("6011");
        message.setPOSDataDE61("1010010000500826T3W 1NL");
        list.add(new Object[]{message, TransactionType.ATM});

        message = new Message();
        message.setMCCCode("5812");
        message.setPOSDataDE61("0260000000009300826WD61JD                 ");
        list.add(new Object[]{message, TransactionType.POS});

        message = new Message();
        message.setMCCCode("5942");
        message.setPOSDataDE61("00000000003002081570");
        list.add(new Object[]{message, TransactionType.POS});

        message = new Message();
        message.setMCCCode("5942");
        message.setPOSDataDE61("1025108006600372DUBLIN 2");
        list.add(new Object[]{message, TransactionType.ONLINE});

        message = new Message();
        message.setMCCCode("5942");
        message.setPOSDataDE61("\n\t\t\t");
        list.add(new Object[]{message, TransactionType.POS});

        return list;
    }

    private Message message;

    private TransactionType expectedTransactionType;

    private TransactionTypeResolver typeResolver;

    public TransactionTypeResolverTest(Message status, TransactionType expectedIsValid) {
        this.message = status;
        this.expectedTransactionType = expectedIsValid;
    }

    @Before
    public void initialize() {
        typeResolver = new TransactionTypeResolver();
    }

    @Test
    public void getTypeShouldReturnCorrectValue() {
        // arrange

        // act
        TransactionType result = typeResolver.getType(message);

        // assert
        assertNotNull(result);
        assertEquals(expectedTransactionType, result);
    }
}
