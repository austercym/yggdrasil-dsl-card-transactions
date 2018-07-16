package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionMatching;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionMatchingTags;

import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.lang3.Validate.notNull;

public class TransactionMatchingTagsGenerator {

    public List<TransactionMatching> createLookupTags(MessageProcessed messageProcessed) {
        notNull(messageProcessed);
        notNull(messageProcessed.getTransactionId());
        Message request = messageProcessed.getRequest();
        notNull(request);

        List<TransactionMatching> list = new ArrayList<>();

        list.add(createTransactionMatching(messageProcessed.getTransactionId(), request, TransactionMatchingTags.create()
                .withTraceIdLifecycle(request.getTraceidLifecycle())));

        list.add(createTransactionMatching(messageProcessed.getTransactionId(), request, TransactionMatchingTags.create()
                .withTransLink(request.getTransLink())));

        list.add(createTransactionMatching(messageProcessed.getTransactionId(), request, TransactionMatchingTags.create()
                .withTraceIdLifecycle(request.getTraceidLifecycle())
                .withTransLink(request.getTransLink())
                .withAuthCode(request.getAuthCodeDE38())));

        list.add(createTransactionMatching(messageProcessed.getTransactionId(), request, TransactionMatchingTags.create()
                .withTransLink(request.getTransLink())
                .withAuthCode(request.getAuthCodeDE38())));

        list.add(createTransactionMatching(messageProcessed.getTransactionId(), request, TransactionMatchingTags.create()
                .withTraceIdLifecycle(request.getTraceidLifecycle())
                .withAuthCode(request.getAuthCodeDE38())));

        list.add(createTransactionMatching(messageProcessed.getTransactionId(), request, TransactionMatchingTags.create()
                .withTraceIdLifecycle(request.getTraceidLifecycle())
                .withTransLink(request.getTransLink())));

        list.add(createTransactionMatching(messageProcessed.getTransactionId(), request, TransactionMatchingTags.create()
                .withAcquirerReferenceData(request.getAcquirerReferenceData031())
                .withTransactionCurrency(request.getTxnCCy())
                .withAuthCode(request.getAuthCodeDE38())
                .withPosTime(request.getPOSTimeDE12())
                .withRetRefNo(request.getRetRefNoDE37())));

        list.add(createTransactionMatching(messageProcessed.getTransactionId(), request, TransactionMatchingTags.create()
                .withAcquirerReferenceData(request.getAcquirerReferenceData031())
                .withAuthCode(request.getAuthCodeDE38())
                .withTransLink(request.getTransLink())));

        return list;
    }

    private TransactionMatching createTransactionMatching(
            String transactionId, Message request, TransactionMatchingTags matchingTags) {
        TransactionMatching matching = new TransactionMatching();
        matching.setTransactionId(transactionId);
        matching.setCardToken(request.getToken());
        matching.setTags(matchingTags.buildMap());
        return matching;
    }
}
