package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionEarmark;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;

import static org.junit.Assert.*;
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
    }

    @Test
    public void recalculateWhenAuthorisationTodayTransactionAndTodayTotalSpendShouldIncreaseDailyAndAnnualTotalSpend()
            throws ParseException {
        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2015-09-19 19:09:42").getTime());
        messageProcessed.setEarmarkAmount(DecimalTypeUtils.toDecimal(-9));

        SpendingTotalAmounts lastSpendingTotalAmounts = new SpendingTotalAmounts();
        lastSpendingTotalAmounts.setTimestamp(Instant.parse("2015-09-19T07:55:42Z"));
        lastSpendingTotalAmounts.setDailyTotal(BigDecimal.valueOf(19));
        lastSpendingTotalAmounts.setAnnualTotal(BigDecimal.valueOf(2015));

        Instant mockedNow = Instant.parse("2015-09-19t21:15:20Z");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, lastSpendingTotalAmounts, null);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.valueOf(28).compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.valueOf(2024).compareTo(result.getAnnualTotal()) == 0);
    }

    @Test
    public void recalculateWhenAuthorisationTodayTransactionAndYesterdayTotalSpendShouldSetDailyAndIncreaseAnnualTotalSpend()
            throws ParseException {
        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2015-09-19 19:09:42").getTime());
        messageProcessed.setEarmarkAmount(DecimalTypeUtils.toDecimal(-9));

        SpendingTotalAmounts lastSpendingTotalAmounts = new SpendingTotalAmounts();
        lastSpendingTotalAmounts.setTimestamp(Instant.parse("2015-09-18T21:20:42Z"));
        lastSpendingTotalAmounts.setDailyTotal(BigDecimal.valueOf(19));
        lastSpendingTotalAmounts.setAnnualTotal(BigDecimal.valueOf(2015));

        Instant mockedNow = Instant.parse("2015-09-19T21:15:20Z");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, lastSpendingTotalAmounts, null);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.valueOf(9).compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.valueOf(2024).compareTo(result.getAnnualTotal()) == 0);
    }

    @Test
    public void recalculateWhenAuthorisationTodayTransactionAndTheYearBeforeTotalSpendShouldSetDailyAndAnnualTotalSpend()
            throws ParseException {
        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2015-09-19 19:09:42").getTime());
        messageProcessed.setEarmarkAmount(DecimalTypeUtils.toDecimal(-9));

        SpendingTotalAmounts lastSpendingTotalAmounts = new SpendingTotalAmounts();
        lastSpendingTotalAmounts.setTimestamp(Instant.parse("2014-09-18T21:20:42Z"));
        lastSpendingTotalAmounts.setDailyTotal(BigDecimal.valueOf(19));
        lastSpendingTotalAmounts.setAnnualTotal(BigDecimal.valueOf(2015));

        Instant mockedNow = Instant.parse("2015-09-19T21:15:20Z");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, lastSpendingTotalAmounts, null);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.valueOf(9).compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.valueOf(9).compareTo(result.getAnnualTotal()) == 0);
    }

    @Test
    public void recalculateWhenAuthorisationTodayTransactionAndTotalSpendNotPresentShouldSetDailyAndAnnualTotalSpend()
            throws ParseException {
        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2015-09-19 19:09:42").getTime());
        messageProcessed.setEarmarkAmount(DecimalTypeUtils.toDecimal(-9));

        Instant mockedNow = Instant.parse("2015-09-19T21:15:20Z");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, null, null);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.valueOf(9).compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.valueOf(9).compareTo(result.getAnnualTotal()) == 0);
    }

    @Test
    public void recalculateWhenPresentmentYesterdayTransactionAndTodayTotalSpendShouldIncreaseAnnualTotalSpendOnly()
            throws ParseException {
        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2015-09-18 19:09:42").getTime());
        messageProcessed.setClientAmount(DecimalTypeUtils.toDecimal(-9));

        SpendingTotalAmounts lastSpendingTotalAmounts = new SpendingTotalAmounts();
        lastSpendingTotalAmounts.setTimestamp(Instant.parse("2015-09-19T07:55:42Z"));
        lastSpendingTotalAmounts.setDailyTotal(BigDecimal.valueOf(19));
        lastSpendingTotalAmounts.setAnnualTotal(BigDecimal.valueOf(2015));

        Instant mockedNow = Instant.parse("2015-09-19T21:15:20Z");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, lastSpendingTotalAmounts, null);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.valueOf(19).compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.valueOf(2024).compareTo(result.getAnnualTotal()) == 0);
    }

    @Test
    public void recalculateWhenPresentmentTheYearBeforeTransactionAndTodayTotalSpendShouldNotChangeAmounts()
            throws ParseException {
        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2014-09-19 19:09:42").getTime());
        messageProcessed.setClientAmount(DecimalTypeUtils.toDecimal(-9));

        SpendingTotalAmounts lastSpendingTotalAmounts = new SpendingTotalAmounts();
        lastSpendingTotalAmounts.setTimestamp(Instant.parse("2015-09-19T07:55:42Z"));
        lastSpendingTotalAmounts.setDailyTotal(BigDecimal.valueOf(19));
        lastSpendingTotalAmounts.setAnnualTotal(BigDecimal.valueOf(2015));

        Instant mockedNow = Instant.parse("2015-09-19T21:15:20Z");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, lastSpendingTotalAmounts, null);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.valueOf(19).compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.valueOf(2015).compareTo(result.getAnnualTotal()) == 0);
    }

    @Test
    public void recalculateWhenPresentmentTheYearBeforeTransactionAndYesterdayTotalSpendShouldSetDailyTotalToZero()
            throws ParseException {
        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2014-09-19 19:09:42").getTime());
        messageProcessed.setClientAmount(DecimalTypeUtils.toDecimal(-9));

        SpendingTotalAmounts lastSpendingTotalAmounts = new SpendingTotalAmounts();
        lastSpendingTotalAmounts.setTimestamp(Instant.parse("2015-09-18T07:55:42Z"));
        lastSpendingTotalAmounts.setDailyTotal(BigDecimal.valueOf(19));
        lastSpendingTotalAmounts.setAnnualTotal(BigDecimal.valueOf(2015));

        Instant mockedNow = Instant.parse("2015-09-19T21:15:20Z");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, lastSpendingTotalAmounts, null);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.ZERO.compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.valueOf(2015).compareTo(result.getAnnualTotal()) == 0);
    }

    @Test
    public void recalculateWhenPresentmentTheYearBeforeTransactionAndTheYearBeforeTotalSpendShouldSetAmountsToZero()
            throws ParseException {
        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2014-09-19 19:09:42").getTime());
        messageProcessed.setClientAmount(DecimalTypeUtils.toDecimal(-9));

        SpendingTotalAmounts lastSpendingTotalAmounts = new SpendingTotalAmounts();
        lastSpendingTotalAmounts.setTimestamp(Instant.parse("2014-09-19T07:55:42Z"));
        lastSpendingTotalAmounts.setDailyTotal(BigDecimal.valueOf(19));
        lastSpendingTotalAmounts.setAnnualTotal(BigDecimal.valueOf(2015));

        Instant mockedNow = Instant.parse("2015-09-19T21:15:20Z");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, lastSpendingTotalAmounts, null);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.ZERO.compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.ZERO.compareTo(result.getAnnualTotal()) == 0);
    }

    @Test
    public void recalculateWhenPresentmentEarmarkEqualClientAmountShouldNotChangeAmounts()
            throws ParseException {

        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2015-09-19 19:09:42").getTime());
        messageProcessed.setClientAmount(DecimalTypeUtils.toDecimal(-9));

        SpendingTotalAmounts lastSpendingTotalAmounts = new SpendingTotalAmounts();
        lastSpendingTotalAmounts.setTimestamp(Instant.parse("2015-09-19T07:55:42Z"));
        lastSpendingTotalAmounts.setDailyTotal(BigDecimal.valueOf(19));
        lastSpendingTotalAmounts.setAnnualTotal(BigDecimal.valueOf(2015));

        TransactionEarmark earmark = new TransactionEarmark();
        earmark.setAmount(BigDecimal.valueOf(9));

        Instant mockedNow = Instant.parse("2015-09-19t21:15:20Z");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, lastSpendingTotalAmounts, earmark);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.valueOf(19).compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.valueOf(2015).compareTo(result.getAnnualTotal()) == 0);
    }

    @Test
    public void recalculateWhenPresentmentEarmarkGreaterThenClientAmountShouldDecreaseAmounts()
            throws ParseException {

        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2015-09-19 19:09:42").getTime());
        messageProcessed.setClientAmount(DecimalTypeUtils.toDecimal(-9));

        SpendingTotalAmounts lastSpendingTotalAmounts = new SpendingTotalAmounts();
        lastSpendingTotalAmounts.setTimestamp(Instant.parse("2015-09-19T07:55:42Z"));
        lastSpendingTotalAmounts.setDailyTotal(BigDecimal.valueOf(19));
        lastSpendingTotalAmounts.setAnnualTotal(BigDecimal.valueOf(2015));

        TransactionEarmark earmark = new TransactionEarmark();
        earmark.setAmount(BigDecimal.valueOf(19));

        Instant mockedNow = Instant.parse("2015-09-19t21:15:20Z");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, lastSpendingTotalAmounts, earmark);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.valueOf(9).compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.valueOf(2005).compareTo(result.getAnnualTotal()) == 0);
    }

    @Test
    public void recalculateWhenPresentmentEarmarkLowerThenClientAmountShouldIncreaseAmounts()
            throws ParseException {

        // arrange
        GpsMessageProcessed messageProcessed = new GpsMessageProcessed();
        messageProcessed.setTransactionTimestamp(sdf.parse("2015-09-19 19:09:42").getTime());
        messageProcessed.setClientAmount(DecimalTypeUtils.toDecimal(-9));

        SpendingTotalAmounts lastSpendingTotalAmounts = new SpendingTotalAmounts();
        lastSpendingTotalAmounts.setTimestamp(Instant.parse("2015-09-19T07:55:42Z"));
        lastSpendingTotalAmounts.setDailyTotal(BigDecimal.valueOf(19));
        lastSpendingTotalAmounts.setAnnualTotal(BigDecimal.valueOf(2015));

        TransactionEarmark earmark = new TransactionEarmark();
        earmark.setAmount(BigDecimal.valueOf(3));

        Instant mockedNow = Instant.parse("2015-09-19t21:15:20Z");
        when(dateTimeServiceMock.now()).thenReturn(mockedNow);

        // act
        SpendingTotalAmounts result = calculator.recalculate(messageProcessed, lastSpendingTotalAmounts, earmark);

        // assert
        assertNotNull(result);
        assertEquals(mockedNow, result.getTimestamp());
        assertTrue(BigDecimal.valueOf(25).compareTo(result.getDailyTotal()) == 0);
        assertTrue(BigDecimal.valueOf(2021).compareTo(result.getAnnualTotal()) == 0);
    }
}
