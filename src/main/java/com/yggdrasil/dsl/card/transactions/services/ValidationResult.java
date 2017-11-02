package com.yggdrasil.dsl.card.transactions.services;

public final class ValidationResult {

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
