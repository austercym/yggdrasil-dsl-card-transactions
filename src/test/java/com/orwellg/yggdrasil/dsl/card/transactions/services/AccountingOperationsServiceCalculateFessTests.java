package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.FeeSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@RunWith(Parameterized.class)
public class AccountingOperationsServiceCalculateFessTests {

    private AccountingOperationsService service = new AccountingOperationsService();

    //parameters
    private BigDecimal amount;
    private BigDecimal percentage;
    private BigDecimal transactionAmount;
    private BigDecimal result;

    @Parameterized.Parameters
    public static List<Object[]> data(){

        List<Object[]> list = new ArrayList<>();

        list.add(new Object[] { BigDecimal.valueOf(12.99), null, BigDecimal.valueOf(10), BigDecimal.valueOf(12.99) } );
        list.add(new Object[] { BigDecimal.valueOf(12.99), BigDecimal.valueOf(0), BigDecimal.valueOf(10), BigDecimal.valueOf(12.99) } );
        list.add(new Object[] { BigDecimal.valueOf(12.99), BigDecimal.valueOf(10), BigDecimal.valueOf(10), BigDecimal.valueOf(13.99) } );

        list.add(new Object[] { BigDecimal.valueOf(12.99), BigDecimal.valueOf(10), BigDecimal.valueOf(0), BigDecimal.valueOf(12.99) } );

        return list;
    }

    public AccountingOperationsServiceCalculateFessTests(BigDecimal amount, BigDecimal percentage, BigDecimal transactionAmount, BigDecimal result){
        this.amount = amount;
        this.percentage = percentage;
        this.transactionAmount = transactionAmount;
        this.result = result;
    }

    @Before
    public void SetUp(){


    }


    @Test
    public void canGetFeeAmount() {

        FeeSchema fees = new FeeSchema();
        fees.setAmount(this.amount);
        fees.setPercentage(this.percentage);

        BigDecimal feeValue =  service.getFeeAmount(fees, this.transactionAmount);

        Assert.assertEquals(feeValue, this.result);

    }

}
