package com.orwellg.yggdrasil.dsl.card.transactions.presentment.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.LinkedAccount;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class LinkedAccountValidationService {


    public LinkedAccount getLast(List<LinkedAccount> linkedAccounts){

        LinkedAccount linkedAccount = null;
        Optional<LinkedAccount> maxDateOptional = linkedAccounts.stream()
                .max(Comparator.comparing(LinkedAccount::getFromTimestamp));

        if (maxDateOptional.isPresent()){
            linkedAccount = maxDateOptional.get();
        }

       return linkedAccount;
    }

}
