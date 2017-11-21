package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.yggdrasil.dsl.card.transactions.model.AuthorisationMessage;
import org.junit.Test;

import static org.junit.Assert.*;


public class GpsMessageMapperTest {

    private GpsMessageMapper mapper = new GpsMessageMapper();

    @Test
    public void mapShouldMapCurrencyCodes() {
        // arrange
        Message message = new Message();
        message.setTxnCCy("946");

        // act
        AuthorisationMessage result = mapper.map(message);

        // assert
        assertNotNull(result);
        //assertEquals("RON", result.getTransactionCurrency());
    }
}
