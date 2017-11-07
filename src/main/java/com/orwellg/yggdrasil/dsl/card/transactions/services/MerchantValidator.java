package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;
import com.orwellg.yggdrasil.dsl.card.transactions.model.AuthorisationMessage;

import java.util.Date;
import java.util.Map;

public class MerchantValidator implements AuthorisationValidator {

    @Override
    public ValidationResult validate(AuthorisationMessage message, CardSettings cardSettings) {

        if (message.getIsCardPresent()) {
            return ValidationResult.valid();
        }
        Map<String, Date> merchants = cardSettings == null
                ? null
                : cardSettings.getAllowedCardNotPresentMerchants();
        String merchantId = message.getMerchantId();
        merchantId = merchantId == null
                ? null
                : merchantId.trim();
        if (merchantId == null || merchantId.isEmpty()){
            return ValidationResult.error("Merchant id is required for card not present transactions");
        }
        Boolean merchantExists = merchants != null && merchants.containsKey(merchantId);
        if (!merchantExists) {
            return ValidationResult.error(String.format(
                    "Card not present transactions from %s merchant are not allowed", merchantId));
        }
        Date permissionExpiry = merchants.get(merchantId);
        if (permissionExpiry != null && new Date().compareTo(permissionExpiry) > 0) {
            return ValidationResult.error(String.format(
                    "Card not present transaction's permission for %s merchant expired", merchantId));
        }
        return ValidationResult.valid();
    }
}
