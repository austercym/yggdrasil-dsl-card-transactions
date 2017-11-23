package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.FeeSchema;
import com.orwellg.yggdrasil.dsl.card.transactions.presentment.services.FeeValidationService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class FeeValidationServiceTest {

    private FeeValidationService service;


    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() {
        service = new FeeValidationService();
    }

    @Test
    public void returnsNullWhenListEmpty() throws Exception {

        List<FeeSchema> list = new ArrayList<>();

        FeeSchema fees = service.getLast(list);

        Assert.assertNull(fees);
    }

    @Test
    public void returnsLast(){

        Calendar c = Calendar.getInstance();
        c.set(2017, 10, 23, 12, 00);

        List<FeeSchema> list = new ArrayList<>();
        FeeSchema fee = new FeeSchema();
        fee.setFromTimestamp(c.getTime());
        list.add(fee);

        c.set(2017, 9, 23, 12, 00);
        FeeSchema fee2 = new FeeSchema();
        fee2.setFromTimestamp(c.getTime());
        list.add(fee2);

        FeeSchema lastFee = service.getLast(list);

        Assert.assertEquals(lastFee, fee);
    }

}
