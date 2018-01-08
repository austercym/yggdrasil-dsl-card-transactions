package com.orwellg.yggdrasil.dsl.card.transactions.authorisation.services;

import java.io.Serializable;

public final class ValidationResult implements Serializable {

    private Boolean isValid;

    private String message;

    public Boolean getIsValid() {
        return isValid;
    }

    public String getMessage() {
        return message;
    }

    private ValidationResult(Boolean isValid, String message) {
        this.isValid = isValid;
        this.message = message;
    }

    public static ValidationResult valid() {
        return new ValidationResult(true, null);
    }

    public static ValidationResult error(String message) {
        return new ValidationResult(false, message);
    }

    @Override
    public String toString() {
        return isValid
                ? "Valid"
                : "Not valid - " + message;
    }
}
