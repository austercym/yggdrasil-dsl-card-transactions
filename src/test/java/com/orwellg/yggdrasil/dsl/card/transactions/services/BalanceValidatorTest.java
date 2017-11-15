package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.accounting.AccountTransactionLog;
import com.orwellg.yggdrasil.dsl.card.transactions.model.AuthorisationMessage;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.*;

public class BalanceValidatorTest {

    private BalanceValidator validator = new BalanceValidator();

    @Test
    public void validateWhenActualBalanceNotExceededShouldReturnValid() {
        // arrange
        AuthorisationMessage message = new AuthorisationMessage();
        message.setSettlementAmount(BigDecimal.valueOf(19.09));
        message.setSettlementCurrency("EUR");

        AccountTransactionLog accountTransactionLog = new AccountTransactionLog();
        accountTransactionLog.setActualBalance(BigDecimal.valueOf(20));
        accountTransactionLog.setLedgerBalance(BigDecimal.valueOf(15));

        // act
        ValidationResult result = validator.validate(message, accountTransactionLog);

        // assert
        assertNotNull(result);
        assertTrue(result.getIsValid());
        assertNull(result.getMessage());
    }

    @Test
    public void validateWhenActualBalanceExceededShouldReturnInvalid() {
        // arrange
        AuthorisationMessage message = new AuthorisationMessage();
        message.setSettlementAmount(BigDecimal.valueOf(19.09));
        message.setSettlementCurrency("EUR");

        AccountTransactionLog accountTransactionLog = new AccountTransactionLog();
        accountTransactionLog.setActualBalance(BigDecimal.valueOf(19));
        accountTransactionLog.setLedgerBalance(BigDecimal.valueOf(42));

        // act
        ValidationResult result = validator.validate(message, accountTransactionLog);

        // assert
        assertNotNull(result);
        assertFalse(result.getIsValid());
        assertNotNull(result.getMessage());
    }
}
