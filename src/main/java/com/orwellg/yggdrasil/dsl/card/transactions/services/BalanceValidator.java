package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.accounting.AccountTransactionLog;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;

public class BalanceValidator {
    public ValidationResult validate(TransactionInfo message, AccountTransactionLog accountTransactionLog) {
        if (accountTransactionLog == null)
            return ValidationResult.error("Account balance not available");

        if (message.getSettlementAmount().compareTo(accountTransactionLog.getActualBalance()) > 0)
            return ValidationResult.error(String.format(
                    "Settlement amount exceeds actual balance (SettlementAmount=%f, ActualBalance=%f)",
                    message.getSettlementAmount(), accountTransactionLog.getActualBalance()));

        return ValidationResult.valid();
    }
}
