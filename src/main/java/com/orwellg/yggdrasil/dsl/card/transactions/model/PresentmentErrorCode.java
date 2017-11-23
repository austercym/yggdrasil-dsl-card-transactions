package com.orwellg.yggdrasil.dsl.card.transactions.model;

public enum PresentmentErrorCode{
    INVALID_PREVIOUS_TRANSACTION_TYPE(101),
    DUPLICATED_TRANSACTION_ID(102),
    FEE_SCHEMA_MISSING(103),
    LINKED_ACCOUNT_MISSING(104);

    private final int number;

    private PresentmentErrorCode(int number){
        this.number = number;
    }

    public int getNumber(){
        return this.number;
    }


}