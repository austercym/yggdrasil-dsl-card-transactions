package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.yggdrasil.dsl.card.transactions.model.CreditDebit;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import static org.junit.Assert.*;


public class CardMessageMapperTest {

    private CardMessageMapper mapper = new CardMessageMapper();

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

    @Test
    public void mapWhenTxnGPSDatePopulatedShouldMapToGpsTransactionTime() {
        // arrange
        Message message = new Message();
        message.setTxnGPSDate("2015-09-19 07:55:42.053");

        // act
        TransactionInfo result = mapper.map(message);

        // assert
        LocalDateTime expected = LocalDateTime.of(2015, 9, 19, 7, 55, 42, 53000000);
        assertNotNull(result);
        assertEquals(expected, result.getProviderTransactionTime());
    }
}
