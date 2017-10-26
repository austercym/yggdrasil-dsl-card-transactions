package com.yggdrasil.dsl.card.transactions.topology.bolts.event;

import com.orwellg.umbrella.commons.storm.topology.component.bolt.BasicRichBolt;
import com.orwellg.umbrella.commons.types.beans.EventAccountingData;
import org.apache.storm.tuple.Tuple;

public class PresentmentAccounting extends BasicRichBolt {


    @Override
    public void declareFieldsDefinition() {

    }

    @Override
    public void execute(Tuple tuple) {
        //create EventAccountingData

        //EventAccountingData accountingData = new EventAccountingData();
        //accountingData.setCdtrProduct(); //creditor product? propably an account -> ask
        //EventAccountingData.ProductData
        //accountingData.setDbtrProduct(); //debtor product?
        //accountingData.setAmount(); //from data
        //accountingData.setCdtrAccountData();
        //EventAccountingData.AccountData
        //accountingData.setDbtrAccountData();
        //accountingData.setMessage(); //message?

    }
}
