package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionMatching;
import com.orwellg.yggdrasil.card.transaction.commons.CurrencyMapper;
import org.apache.commons.lang3.StringUtils;
import org.modelmapper.Converter;
import org.modelmapper.ModelMapper;
import org.modelmapper.convention.MatchingStrategies;

public final class MapperFactory {
    public static ModelMapper getMapper() {
        ModelMapper mapper = new ModelMapper();

        mapper.getConfiguration().setMatchingStrategy(MatchingStrategies.STRICT);

        CurrencyMapper currencyMapper = new CurrencyMapper();
        Converter<String, String> currencyConverter = ctx -> StringUtils.isBlank(ctx.getSource())
                ? null
                : currencyMapper.currencyFromNumericCode(ctx.getSource());

        mapper.createTypeMap(MessageProcessed.class, TransactionMatching.class)
                .addMappings(m -> {
                    m.map(src -> src.getRequest().getToken(), TransactionMatching::setCardToken);
                    m.map(src -> src.getRequest().getTraceidLifecycle(), TransactionMatching::setTraceIdLifecycle);
                    m.map(src -> src.getRequest().getTransLink(), TransactionMatching::setTransLink);
                    m.map(src -> src.getRequest().getAuthCodeDE38(), TransactionMatching::setAuthCode);
                    m.map(src -> src.getRequest().getAcquirerReferenceData031(), TransactionMatching::setAcquirerReferenceData);
                    m.map(src -> src.getRequest().getTxnAmt(), TransactionMatching::setTransactionAmount);
                    m.using(currencyConverter)
                            .map(src -> src.getRequest().getTxnCCy(), TransactionMatching::setTransactionCurrency);
                    m.map(src -> src.getRequest().getPOSTimeDE12(), TransactionMatching::setPosTime);
                    m.map(src -> src.getRequest().getRetRefNoDE37(), TransactionMatching::setRetRefNo);
                    m.map(src -> src.getRequest().getTXnID(), TransactionMatching::setMessageId);
                });
        return mapper;
    }
}
