package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TotalSpendAmountsCalculatorTest {

    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private TotalSpendAmountsCalculator calculator;

    @Mock
    private DateTimeService dateTimeServiceMock;

    @Before
    public void setUp() {
        calculator = new TotalSpendAmountsCalculator(dateTimeServiceMock);

        when(dateTimeServiceMock.getDatePart(any())).thenCallRealMethod();
        when(dateTimeServiceMock.getYearPart(any())).thenCallRealMethod();
    }

    @Test
    public void recalculateWhenTodayTransactionAndTodayTotalSpendShouldIncreaseDailyAndAnnualTotalSpend()
            throws ParseException {
        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2015-09-19 19:09:42").getTime());
        messageProcessed.setBlockedClientAmount(DecimalTypeUtils.toDecimal(9));

        SpendingTotalAmounts lastSpendingTotalAmounts = new SpendingTotalAmounts();
        lastSpendingTotalAmounts.setTimestamp(sdf.parse("2015-09-19 07:55:42"));
        lastSpendingTotalAmounts.setDailyTotal(BigDecimal.valueOf(19));
        lastSpendingTotalAmounts.setAnnualTotal(BigDecimal.valueOf(2015));

        Date mockedNow = sdf.parse("2015-09-19 21:15:20");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, lastSpendingTotalAmounts);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.valueOf(28).compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.valueOf(2024).compareTo(result.getAnnualTotal()) == 0);
    }

    @Test
    public void recalculateWhenTodayTransactionAndYesterdayTotalSpendShouldSetDailyAndIncreaseAnnualTotalSpend()
            throws ParseException {
        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2015-09-19 19:09:42").getTime());
        messageProcessed.setBlockedClientAmount(DecimalTypeUtils.toDecimal(9));

        SpendingTotalAmounts lastSpendingTotalAmounts = new SpendingTotalAmounts();
        lastSpendingTotalAmounts.setTimestamp(sdf.parse("2015-09-18 21:20:42"));
        lastSpendingTotalAmounts.setDailyTotal(BigDecimal.valueOf(19));
        lastSpendingTotalAmounts.setAnnualTotal(BigDecimal.valueOf(2015));

        Date mockedNow = sdf.parse("2015-09-19 21:15:20");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, lastSpendingTotalAmounts);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.valueOf(9).compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.valueOf(2024).compareTo(result.getAnnualTotal()) == 0);
    }

    @Test
    public void recalculateWhenTodayTransactionAndTheYearBeforeTotalSpendShouldSetDailyAndAnnualTotalSpend()
            throws ParseException {
        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2015-09-19 19:09:42").getTime());
        messageProcessed.setBlockedClientAmount(DecimalTypeUtils.toDecimal(9));

        SpendingTotalAmounts lastSpendingTotalAmounts = new SpendingTotalAmounts();
        lastSpendingTotalAmounts.setTimestamp(sdf.parse("2014-09-18 21:20:42"));
        lastSpendingTotalAmounts.setDailyTotal(BigDecimal.valueOf(19));
        lastSpendingTotalAmounts.setAnnualTotal(BigDecimal.valueOf(2015));

        Date mockedNow = sdf.parse("2015-09-19 21:15:20");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, lastSpendingTotalAmounts);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.valueOf(9).compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.valueOf(9).compareTo(result.getAnnualTotal()) == 0);
    }

    @Test
    public void recalculateWhenTodayTransactionAndTotalSpendNotPresentShouldSetDailyAndAnnualTotalSpend()
            throws ParseException {
        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2015-09-19 19:09:42").getTime());
        messageProcessed.setBlockedClientAmount(DecimalTypeUtils.toDecimal(9));

        Date mockedNow = sdf.parse("2015-09-19 21:15:20");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, null);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.valueOf(9).compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.valueOf(9).compareTo(result.getAnnualTotal()) == 0);
    }

    @Test
    public void recalculateWhenYesterdayTransactionAndTodayTotalSpendShouldIncreaseAnnualTotalSpendOnly()
            throws ParseException {
        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2015-09-18 19:09:42").getTime());
        messageProcessed.setBlockedClientAmount(DecimalTypeUtils.toDecimal(9));

        SpendingTotalAmounts lastSpendingTotalAmounts = new SpendingTotalAmounts();
        lastSpendingTotalAmounts.setTimestamp(sdf.parse("2015-09-19 07:55:42"));
        lastSpendingTotalAmounts.setDailyTotal(BigDecimal.valueOf(19));
        lastSpendingTotalAmounts.setAnnualTotal(BigDecimal.valueOf(2015));

        Date mockedNow = sdf.parse("2015-09-19 21:15:20");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, lastSpendingTotalAmounts);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.valueOf(19).compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.valueOf(2024).compareTo(result.getAnnualTotal()) == 0);
    }

    @Test
    public void recalculateWhenTheYearBeforeTransactionAndTodayTotalSpendShouldNotChangeAmounts()
            throws ParseException {
        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2014-09-19 19:09:42").getTime());
        messageProcessed.setBlockedClientAmount(DecimalTypeUtils.toDecimal(9));

        SpendingTotalAmounts lastSpendingTotalAmounts = new SpendingTotalAmounts();
        lastSpendingTotalAmounts.setTimestamp(sdf.parse("2015-09-19 07:55:42"));
        lastSpendingTotalAmounts.setDailyTotal(BigDecimal.valueOf(19));
        lastSpendingTotalAmounts.setAnnualTotal(BigDecimal.valueOf(2015));

        Date mockedNow = sdf.parse("2015-09-19 21:15:20");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, lastSpendingTotalAmounts);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.valueOf(19).compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.valueOf(2015).compareTo(result.getAnnualTotal()) == 0);
    }

    @Test
    public void recalculateWhenTheYearBeforeTransactionAndYesterdayTotalSpendShouldSetDailyTotalToZero()
            throws ParseException {
        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2014-09-19 19:09:42").getTime());
        messageProcessed.setBlockedClientAmount(DecimalTypeUtils.toDecimal(9));

        SpendingTotalAmounts lastSpendingTotalAmounts = new SpendingTotalAmounts();
        lastSpendingTotalAmounts.setTimestamp(sdf.parse("2015-09-18 07:55:42"));
        lastSpendingTotalAmounts.setDailyTotal(BigDecimal.valueOf(19));
        lastSpendingTotalAmounts.setAnnualTotal(BigDecimal.valueOf(2015));

        Date mockedNow = sdf.parse("2015-09-19 21:15:20");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, lastSpendingTotalAmounts);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.ZERO.compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.valueOf(2015).compareTo(result.getAnnualTotal()) == 0);
    }

    @Test
    public void recalculateWhenTheYearBeforeTransactionAndTheYearBeforeTotalSpendShouldSetAmountsToZero()
            throws ParseException {
        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2014-09-19 19:09:42").getTime());
        messageProcessed.setBlockedClientAmount(DecimalTypeUtils.toDecimal(9));

        SpendingTotalAmounts lastSpendingTotalAmounts = new SpendingTotalAmounts();
        lastSpendingTotalAmounts.setTimestamp(sdf.parse("2014-09-19 07:55:42"));
        lastSpendingTotalAmounts.setDailyTotal(BigDecimal.valueOf(19));
        lastSpendingTotalAmounts.setAnnualTotal(BigDecimal.valueOf(2015));

        Date mockedNow = sdf.parse("2015-09-19 21:15:20");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, lastSpendingTotalAmounts);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.ZERO.compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.ZERO.compareTo(result.getAnnualTotal()) == 0);
    }
}
