package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.yggdrasil.dsl.card.transactions.model.CreditDebit;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.*;


public class GpsMessageMapperTest {

    private GpsMessageMapper mapper = new GpsMessageMapper();

    @Test
    public void mapShouldMapCurrencyCodes() {
        // arrange
        Message message = new Message();
        message.setTxnCCy("946");

        // act
        TransactionInfo result = mapper.map(message);

        // assert
        assertNotNull(result);
        assertEquals("RON", result.getTransactionCurrency());
    }

    @Test
    public void mapShouldMapSettlementAmountAndCurrency() {
        // arrange
        Message message = new Message();
        message.setSettleAmt(-19.09);
        message.setSettleCcy("985");

        // act
        TransactionInfo result = mapper.map(message);

        // assert
        assertNotNull(result);
        assertTrue(BigDecimal.valueOf(-19.09).compareTo(result.getSettlementAmount()) == 0);
        assertEquals("PLN", result.getSettlementCurrency());
        assertEquals(CreditDebit.DEBIT, result.getCreditDebit());
    }
}
