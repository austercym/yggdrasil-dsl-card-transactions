package com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services;

import com.orwellg.umbrella.commons.types.scylla.entities.accounting.AccountTransactionLog;
import com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services.BalanceValidator;
import com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services.ValidationResult;
import com.orwellg.yggdrasil.dsl.card.transactions.model.TransactionInfo;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;

import static org.junit.Assert.*;

public class BalanceValidatorTest {

    private BalanceValidator validator = new BalanceValidator();

    @Test
    public void validateWhenActualBalanceNotExceededShouldReturnValid() {
        // arrange
        TransactionInfo message = new TransactionInfo();
        message.setSettlementAmount(BigDecimal.valueOf(19.09));
        message.setSettlementCurrency("EUR");

        AccountTransactionLog accountTransactionLog = new AccountTransactionLog();
        accountTransactionLog.setActualBalance(BigDecimal.valueOf(20));
        accountTransactionLog.setLedgerBalance(BigDecimal.valueOf(15));

        // act
        ValidationResult result = validator.validate(message, accountTransactionLog);

        // assert
        Assert.assertNotNull(result);
        Assert.assertTrue(result.getIsValid());
        Assert.assertNull(result.getMessage());
    }

    @Test
    public void validateWhenActualBalanceExceededShouldReturnInvalid() {
        // arrange
        TransactionInfo message = new TransactionInfo();
        message.setSettlementAmount(BigDecimal.valueOf(19.09));
        message.setSettlementCurrency("EUR");

        AccountTransactionLog accountTransactionLog = new AccountTransactionLog();
        accountTransactionLog.setActualBalance(BigDecimal.valueOf(19));
        accountTransactionLog.setLedgerBalance(BigDecimal.valueOf(42));

        // act
        ValidationResult result = validator.validate(message, accountTransactionLog);

        // assert
        Assert.assertNotNull(result);
        Assert.assertFalse(result.getIsValid());
        Assert.assertNotNull(result.getMessage());
    }

    @Test
    public void validateWhenNoBalanceInformationShouldReturnInvalid() {
        // arrange
        TransactionInfo message = new TransactionInfo();
        message.setSettlementAmount(BigDecimal.valueOf(19.09));
        message.setSettlementCurrency("EUR");

        // act
        ValidationResult result = validator.validate(message, null);

        // assert
        Assert.assertNotNull(result);
        Assert.assertFalse(result.getIsValid());
        Assert.assertNotNull(result.getMessage());
    }
}
