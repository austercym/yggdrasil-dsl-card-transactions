package com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.AccountBalance;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;

public class BalanceValidator {
    public ValidationResult validate(TransactionInfo message, AccountBalance accountBalance) {
        if (accountBalance == null)
            return ValidationResult.error("Account balance not available");

        if (message.getSettlementAmount().abs().compareTo(accountBalance.getActualBalance()) > 0)
            return ValidationResult.error(String.format(
                    "Settlement amount exceeds actual balance (SettlementAmount=%f, ActualBalance=%f)",
                    message.getSettlementAmount(), accountBalance.getActualBalance()));

        return ValidationResult.valid();
    }
}
