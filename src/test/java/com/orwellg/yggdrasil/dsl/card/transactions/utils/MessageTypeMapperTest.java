package com.orwellg.yggdrasil.dsl.card.transactions.utils;

import com.orwellg.umbrella.avro.types.cards.MessageType;
import org.junit.Test;

import static org.junit.Assert.*;

public class MessageTypeMapperTest {

    @Test
    public void fromGpsTxnTypeWhenAShouldReturnAuthorisation() {
        // act
        MessageType result = MessageTypeMapper.fromGpsTxnType("a");

        // assert
        assertEquals(MessageType.AUTHORISATION, result);
    }

    @Test
    public void toGpsTxnTypeWhenPresentmentShouldReturnP() {
        // act
        String result = MessageTypeMapper.toGpsTxnType(MessageType.PRESENTMENT);

        // assert
        assertEquals("P", result);
    }
}