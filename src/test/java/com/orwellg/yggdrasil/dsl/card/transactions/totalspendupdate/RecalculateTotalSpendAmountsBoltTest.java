package com.orwellg.yggdrasil.dsl.card.transactions.totalspendupdate;

import com.orwellg.umbrella.avro.types.gps.GpsMessageProcessed;
import com.orwellg.umbrella.avro.types.gps.ResponseMsg;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.SpendingTotalAmounts;
import com.orwellg.umbrella.commons.types.utils.avro.DecimalTypeUtils;
import com.orwellg.yggdrasil.dsl.card.transactions.services.TotalSpendAmountsCalculator;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.*;

public class RecalculateTotalSpendAmountsBoltTest {

    private RecalculateTotalSpendAmountsBolt bolt;

    @Before
    public void setUp() {
        bolt = new RecalculateTotalSpendAmountsBolt();
        bolt.declareFieldsDefinition();
    }

    @Test
    public void executeWhenAcceptedAuthorisationShouldCalculateNewTotalSpendAmounts() {
        // arrange
        ResponseMsg response = new ResponseMsg();
        response.setResponsestatus("00");

        GpsMessageProcessed message = new GpsMessageProcessed();
        message.setGpsMessageType("A");
        message.setEhiResponse(response);

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(message);

        TotalSpendAmountsCalculator calculator = mock(TotalSpendAmountsCalculator.class);
        when(calculator.recalculate(any(), any(), any())).thenReturn(new SpendingTotalAmounts());
        bolt.setCalculator(calculator);

        OutputCollector collector = mock(OutputCollector.class);
        bolt.setCollector(collector);

        // act
        bolt.execute(input);

        // assert
        verify(collector).emit(
                any(Tuple.class),
                argThat(result -> result.stream().anyMatch(SpendingTotalAmounts.class::isInstance)));
    }

    @Test
    public void executeWhenDeclinedAuthorisationShouldNotCalculateNewTotalSpendAmounts() {
        // arrange
        ResponseMsg response = new ResponseMsg();
        response.setResponsestatus("51");

        GpsMessageProcessed message = new GpsMessageProcessed();
        message.setGpsMessageType("A");
        message.setEhiResponse(response);

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(message);

        TotalSpendAmountsCalculator calculator = mock(TotalSpendAmountsCalculator.class);
        when(calculator.recalculate(any(), any(), any())).thenReturn(new SpendingTotalAmounts());
        bolt.setCalculator(calculator);

        OutputCollector collector = mock(OutputCollector.class);
        bolt.setCollector(collector);

        // act
        bolt.execute(input);

        // assert
        verify(collector).emit(
                any(Tuple.class),
                argThat(result -> result.stream().noneMatch(SpendingTotalAmounts.class::isInstance)));
    }

    @Test
    public void executeWhenDebitPresentmentShouldCalculateNewTotalSpendAmounts() {
        // arrange
        GpsMessageProcessed message = new GpsMessageProcessed();
        message.setGpsMessageType("P");
        message.setClientAmount(DecimalTypeUtils.toDecimal(-19.09));

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(message);

        TotalSpendAmountsCalculator calculator = mock(TotalSpendAmountsCalculator.class);
        when(calculator.recalculate(any(), any(), any())).thenReturn(new SpendingTotalAmounts());
        bolt.setCalculator(calculator);

        OutputCollector collector = mock(OutputCollector.class);
        bolt.setCollector(collector);

        // act
        bolt.execute(input);

        // assert
        verify(collector).emit(
                any(Tuple.class),
                argThat(result -> result.stream().anyMatch(SpendingTotalAmounts.class::isInstance)));
    }

    @Test
    public void executeWhenCreditPresentmentShouldNotCalculateNewTotalSpendAmounts() {
        // arrange
        GpsMessageProcessed message = new GpsMessageProcessed();
        message.setGpsMessageType("P");
        message.setClientAmount(DecimalTypeUtils.toDecimal(19.09));

        Tuple input = mock(Tuple.class);
        when(input.getValueByField(Fields.EVENT_DATA)).thenReturn(message);

        TotalSpendAmountsCalculator calculator = mock(TotalSpendAmountsCalculator.class);
        when(calculator.recalculate(any(), any(), any())).thenReturn(new SpendingTotalAmounts());
        bolt.setCalculator(calculator);

        OutputCollector collector = mock(OutputCollector.class);
        bolt.setCollector(collector);

        // act
        bolt.execute(input);

        // assert
        verify(collector).emit(
                any(Tuple.class),
                argThat(result -> result.stream().noneMatch(SpendingTotalAmounts.class::isInstance)));
    }
}