package com.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.CardSettings;

import java.util.Date;
import java.util.Map;

public class MerchantValidator implements AuthorisationValidator {

    private final CardPresenceResolver cardPresenceResolver;

    public MerchantValidator() {
        this.cardPresenceResolver = new CardPresenceResolver();
    }

    public MerchantValidator(CardPresenceResolver cardPresenceResolver) {
        this.cardPresenceResolver = cardPresenceResolver;
    }

    @Override
    public ValidationResult validate(Message message, CardSettings cardSettings) {

        if (cardPresenceResolver.isCardPresent(message)) {
            return ValidationResult.valid();
        }
        Map<String, Date> merchants = cardSettings == null
                ? null
                : cardSettings.getAllowedCardNotPresentMerchants();
        String merchantId = message.getMerchIDDE42();
        merchantId = merchantId == null
                ? null
                : merchantId.trim();
        if (merchantId == null || merchantId.isEmpty()){
            return ValidationResult.error("Merchant id is required for card not present transactions");
        }
        Boolean merchantExists = merchants.containsKey(merchantId);
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
