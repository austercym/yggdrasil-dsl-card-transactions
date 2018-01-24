package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;
import com.orwellg.yggdrasil.dsl.card.transactions.model.GpsMessageProcessingException;
import com.orwellg.yggdrasil.dsl.card.transactions.presentment.services.AuthorisationValidationService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class AuthorisationValidationServiceTest {

    private AuthorisationValidationService service;


    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        service = new AuthorisationValidationService();
    }

    @Test
    public void returnsNullWhenListEmpty() throws Exception {

        List<CardTransaction> list = new ArrayList<>();

        CardTransaction lastTransation = service.getLast(list, "1");

        Assert.assertNull(lastTransation);
    }

    @Test
    public void throwsExceptionIfLastTransactionIsNotValidType() throws Exception {

        thrown.expect(GpsMessageProcessingException.class);
        thrown.expectMessage("INVALID_PREVIOUS_TRANSACTION_TYPE|101||Error when processing presentment - invalid last transaction type: F. Valid types are: A, D, P");

        Instant now = Instant.now();

        List<CardTransaction> list = new ArrayList<>();
        CardTransaction transaction = new CardTransaction();
        transaction.setGpsMessageType("F");
        transaction.setGpsTransactionId("12");
        transaction.setGpsTransactionLink("1");
        transaction.setTransactionTimestamp(now);
        list.add(transaction);

        CardTransaction lastTransation = service.getLast(list, "1");
    }

    @Test
    public void throwsExceptionIfLastTransactionHasSameId() throws Exception {

        thrown.expect(GpsMessageProcessingException.class);
        thrown.expectMessage("DUPLICATED_TRANSACTION_ID|102||Error when processing presentment - last processed transaction has the same transactionId: 12");

        Instant now = Instant.now();

        List<CardTransaction> list = new ArrayList<>();
        CardTransaction transaction = new CardTransaction();
        transaction.setGpsMessageType("A");
        transaction.setGpsTransactionId("12");
        transaction.setGpsTransactionLink("1");
        transaction.setTransactionTimestamp(now);
        list.add(transaction);

        CardTransaction lastTransation = service.getLast(list, "12");
    }

    @Test
    public void returnLastOfTransactions() throws Exception {

        List<CardTransaction> list = new ArrayList<>();
        CardTransaction transaction = new CardTransaction();
        transaction.setGpsMessageType("A");
        transaction.setGpsTransactionId("12");
        transaction.setGpsTransactionLink("1");
        transaction.setTransactionTimestamp(Instant.parse("2017-10-23T12:00:00Z"));
        list.add(transaction);

        CardTransaction transaction2 = new CardTransaction();
        transaction2.setGpsMessageType("A");
        transaction2.setGpsTransactionId("13");
        transaction2.setGpsTransactionLink("1");
        transaction2.setTransactionTimestamp(Instant.parse("2017-10-24T12:00:00Z"));
        list.add(transaction2);

        CardTransaction lastTransaction = service.getLast(list, "14");

        Assert.assertEquals(transaction2, lastTransaction);
    }




}
