package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.LinkedAccount;
import com.orwellg.yggdrasil.dsl.card.transactions.presentment.services.LinkedAccountValidationService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class LinkedAccountValidationServiceTest {

    private LinkedAccountValidationService service;


    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        service = new LinkedAccountValidationService();
    }

    @Test
    public void retursNullWhenListEmpty(){

        List<LinkedAccount> list = new ArrayList<>();

        LinkedAccount lastAccount = service.getLast(list);

        Assert.assertNull(lastAccount);

    }


    @Test
    public void returnsLastAccount(){

        Calendar c = Calendar.getInstance();
        c.set(2017, 10, 23, 12, 00);

        List<LinkedAccount> list = new ArrayList<>();
        LinkedAccount account = new LinkedAccount();
        account.setDebitCardId(1l);
        account.setFromTimestamp(c.getTime());
        list.add(account);

        c.set(2017, 9, 23, 12, 00);
        LinkedAccount account2 = new LinkedAccount();
        account2.setDebitCardId(1l);
        account2.setFromTimestamp(c.getTime());
        list.add(account2);

        LinkedAccount lastAccount = service.getLast(list);

        Assert.assertEquals(lastAccount, account);
    }



}



