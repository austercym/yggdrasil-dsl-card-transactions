package com.orwellg.yggdrasil.dsl.card.transactions.services;

import java.time.Instant;

public class DateTimeService {
    public Instant now() {
        return Instant.now();
    }
}
