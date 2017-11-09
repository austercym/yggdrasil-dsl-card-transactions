package com.orwellg.yggdrasil.dsl.card.transactions.presentment;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardTransaction;

public class PresentmentAuthorisationService {


    public PresentmentMessage updatePresentment(PresentmentMessage presentment, CardTransaction lastTransaction){

        if (lastTransaction != null){
            presentment.setAuthBlockedClientAmaount(lastTransaction.getBlockedClientAmount());
            presentment.setAuthBlockedClientCurrency(lastTransaction.getBlockedClientCurrency());
            presentment.setAuthWirecardAmount(lastTransaction.getWirecardAmount());
            presentment.setAuthWirecardCurrency(lastTransaction.getWirecardCurrency());
            presentment.setInternalAccountId(lastTransaction.getInternalAccountId());
            presentment.setInternalAccountCurrency(lastTransaction.getInternalAccountCurrency()); //todo:??
            presentment.setAuthFeeAmount(lastTransaction.getFeeAmount());
            presentment.setAuthFeeCurrency(lastTransaction.getInternalAccountCurrency()); //todo: what currency should we get fees?
        }
        return presentment;

    }

}
