package com.orwellg.yggdrasil.dsl.card.transactions.services;

import com.orwellg.umbrella.avro.types.cards.MessageProcessed;
import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.umbrella.commons.types.scylla.entities.cards.TransactionMatching;
import org.junit.Test;
import org.modelmapper.ModelMapper;

import java.math.BigDecimal;

import static org.junit.Assert.*;

public class MapperFactoryTest {

    private ModelMapper mapper = MapperFactory.getMapper();

    @Test
    public void getMapperShouldPassValidation() {
        // arrange

        // act

        // assert
        mapper.validate();
    }

    @Test
    public void getMapperWhenMappingMessageProcessedToTransactionMatchingShouldMapAllFields(){
        // arrange
        MessageProcessed source = new MessageProcessed();
        source.setTransactionId("pas");
        Message request = new Message();
        request.setToken("foo");
        request.setTraceidLifecycle("bar");
        request.setTransLink("buz");
        request.setAuthCodeDE38("xor");
        request.setAcquirerReferenceData031("nor");
        request.setTxnAmt(10.06);
        request.setTxnCCy("985");
        request.setPOSTimeDE12("if");
        request.setRetRefNoDE37("not");
        request.setTXnID("as");
        source.setRequest(request);

        // act
        TransactionMatching result = mapper.map(source, TransactionMatching.class);

        // assert
        assertNotNull(result);
        assertEquals("foo", result.getCardToken());
        assertEquals("bar", result.getTraceIdLifecycle());
        assertEquals("buz", result.getTransLink());
        assertEquals("xor", result.getAuthCode());
        assertEquals("nor", result.getAcquirerReferenceData());
        assertEquals(0, BigDecimal.valueOf(10.06).compareTo(result.getTransactionAmount()));
        assertEquals("PLN", result.getTransactionCurrency());
        assertEquals("if", result.getPosTime());
        assertEquals("not", result.getRetRefNo());
        assertEquals("as", result.getMessageId());
        assertEquals("pas", result.getTransactionId());
    }
}