package com.orwellg.yggdrasil.dsl.card.transactions.presentment.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.FeeSchema;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class FeeValidationService {

    //todo: validate currencies?

    public FeeSchema getLast(List<FeeSchema> fees){

        Optional<FeeSchema> maxDateOptional = fees.stream()
                .max(Comparator.comparing(FeeSchema::getFromTimestamp));

        FeeSchema result = null;
        if(maxDateOptional.isPresent()){
            result = maxDateOptional.get();
        }
        return result;
    }

}
