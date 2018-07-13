package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.google.common.collect.Sets;
import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionMatching;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionMatchingTags;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TransactionMatchingTagsGeneratorTest {

    private final TransactionMatchingTagsGenerator generator = new TransactionMatchingTagsGenerator();

    @Test
    public void createLookupTagsShouldContainAllRequiredTagCombinations() {
        // arrange
        MessageProcessed messageProcessed = new MessageProcessed();
        messageProcessed.setTransactionId("foo");
        Message request = new Message();
        request.setTransLink("tl");
        request.setTraceidLifecycle("ti");
        request.setAuthCodeDE38("ac");
        request.setAcquirerReferenceData031("ar");
        request.setTxnCCy("975");
        request.setPOSTimeDE12("pt");
        request.setRetRefNoDE37("rr");
        messageProcessed.setRequest(request);

        List<HashSet<String>> expectedTagSets = Arrays.asList(

                Sets.newHashSet(
                        TransactionMatchingTags.TRACE_ID_LIFECYCLE),

                Sets.newHashSet(
                        TransactionMatchingTags.TRANS_LINK),

                Sets.newHashSet(
                        TransactionMatchingTags.TRACE_ID_LIFECYCLE,
                        TransactionMatchingTags.TRANS_LINK,
                        TransactionMatchingTags.AUTH_CODE),

                Sets.newHashSet(
                        TransactionMatchingTags.TRANS_LINK,
                        TransactionMatchingTags.AUTH_CODE),

                Sets.newHashSet(
                        TransactionMatchingTags.TRACE_ID_LIFECYCLE,
                        TransactionMatchingTags.AUTH_CODE),

                Sets.newHashSet(
                        TransactionMatchingTags.TRACE_ID_LIFECYCLE,
                        TransactionMatchingTags.TRANS_LINK),

                Sets.newHashSet(
                        TransactionMatchingTags.ACQUIRER_REFERENCE_DATA,
                        TransactionMatchingTags.TRANSACTION_CURRENCY,
                        TransactionMatchingTags.AUTH_CODE,
                        TransactionMatchingTags.POS_TIME,
                        TransactionMatchingTags.RET_REF_NO),

                Sets.newHashSet(
                        TransactionMatchingTags.ACQUIRER_REFERENCE_DATA,
                        TransactionMatchingTags.AUTH_CODE,
                        TransactionMatchingTags.TRANS_LINK)
        );

        // act
        List<TransactionMatching> result = generator.createLookupTags(messageProcessed);

        // assert
        assertNotNull(result);
        expectedTagSets.forEach(expectedTags ->
                assertTrue(result.stream().anyMatch(
                        actual -> {
                            Set<String> actualTags = actual.getTags().keySet();
                            return actualTags.equals(expectedTags);
                        }))
        );
    }
}