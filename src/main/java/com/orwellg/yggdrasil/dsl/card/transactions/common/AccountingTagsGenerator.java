package com.orwellg.yggdrasil.dsl.card.transactions.common;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.avro.types.commons.KeyValue;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.cards.CardAccountingTags;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class AccountingTagsGenerator {

    public static List<KeyValue> getAccountingTags(MessageProcessed processed) {
        Message request = processed.getRequest();
        List<KeyValue> tags = new ArrayList<>();
        if (processed.getMessageType() != null) {
            Collections.addAll(
                    tags,
                    new KeyValue(CardAccountingTags.MESSAGE_TYPE.getTag(), processed.getMessageType().name()));
        }
        if (request != null) {
            Collections.addAll(
                    tags,
                    new KeyValue(CardAccountingTags.AUTH_CODE_DE38.getTag(), request.getAuthCodeDE38()),
                    new KeyValue(CardAccountingTags.RET_REF_NO_DE37.getTag(), request.getRetRefNoDE37()),
                    new KeyValue(CardAccountingTags.TRANS_LINK.getTag(), request.getTransLink()),
                    new KeyValue(CardAccountingTags.TXN_ID.getTag(), request.getTXnID()),
                    new KeyValue(CardAccountingTags.TXN_TYPE.getTag(), request.getTxnType()),
                    new KeyValue(CardAccountingTags.MESSAGE_TYPE_ID.getTag(), request.getMTID()),
                    new KeyValue(CardAccountingTags.PROCESSING_CODE.getTag(), request.getProcCode()));
        }
        return tags;
    }
}
