package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;


@RunWith(Parameterized.class)
public class GpsMessageMapperIsBalanceEnquiryTest {

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { "000000", false },
                { "010000", false },
                { "300000", true }
        });
    }

    private GpsMessageMapper mapper = new GpsMessageMapper();

    private final String procCode;
    private final Boolean expectedIsBalanceEnquiry;

    public GpsMessageMapperIsBalanceEnquiryTest(String procCode, Boolean expectedIsBalanceEnquiry) {
        this.procCode = procCode;
        this.expectedIsBalanceEnquiry = expectedIsBalanceEnquiry;
    }

    @Test
    public void mapShouldMapIsBalanceEnquiry() {
        // arrange
        Message message = new Message();
        message.setProcCode(procCode);

        // act
        TransactionInfo result = mapper.map(message);

        // assert
        assertNotNull(result);
        assertEquals(expectedIsBalanceEnquiry, result.getIsBalanceEnquiry());
    }
}
