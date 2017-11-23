package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.yggdrasil.dsl.card.transactions.presentment.services.ResponseService;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class ResponseServiceTest {

    private ResponseService service;


    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp()
    {
        service = new ResponseService();
    }

    @Test
    public void canCreateResponse(){

        //todo

    }


}
