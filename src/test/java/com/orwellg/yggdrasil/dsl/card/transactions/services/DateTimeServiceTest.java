package com.orwellg.yggdrasil.dsl.card.transactions.services;

import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DateTimeServiceTest {

    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final DateTimeService service = new DateTimeService();

    @Test
    public void getDatePartShouldReturnDatePartOnly() throws ParseException {
        // arrange
        Date date = sdf.parse("2015-09-19 07:55:42");

        // act
        Date result = service.getDatePart(date);

        // assert
        assertNotNull(result);
        assertEquals(sdf.parse("2015-09-19 00:00:00"), result);
    }

    @Test
    public void getYearPartShouldReturnYearOnly() throws ParseException {
        // arrange
        Date date = sdf.parse("2015-09-19 07:55:42");

        // act
        Integer result = service.getYearPart(date);

        // assert
        assertNotNull(result);
        assertEquals(new Integer(2015), result);
    }
}
