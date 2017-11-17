package com.orwellg.yggdrasil.dsl.card.transactions.services;

import java.util.Calendar;
import java.util.Date;

public class DateTimeService {
    public Date now(){
        return new Date();
    }

    public Date getDatePart(Date dateTime) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(dateTime);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return cal.getTime();
    }

    public Integer getYearPart(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        return cal.get(Calendar.YEAR);
    }
}
