package com.orwellg.yggdrasil.dsl.card.transactions.utils;

import com.orwellg.umbrella.avro.types.event.EntityIdentifierType;
import com.orwellg.umbrella.avro.types.event.Event;
import com.orwellg.umbrella.avro.types.event.EventType;
import com.orwellg.umbrella.avro.types.event.ProcessIdentifierType;
import com.orwellg.umbrella.commons.utils.constants.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Date;

public class EventBuilder {

    private static final Logger LOG = LogManager.getLogger(EventBuilder.class);

    public Event buildResponseEvent(String source, String eventName, Object eventData, String responseKey, String parentKey) {

        LOG.debug("Generating event with response data.");

        // Create the event type
        EventType eventType = new EventType();
        eventType.setName(eventName);
        eventType.setVersion(Constants.getDefaultEventVersion());
        eventType.setKey(responseKey);
        eventType.setSource(source);
        eventType.setParentKey(parentKey);
        SimpleDateFormat format = new SimpleDateFormat(Constants.getDefaultEventTimestampFormat());
        eventType.setTimestamp(format.format(new Date()));
        eventType.setData(eventData.toString());

        ProcessIdentifierType processIdentifier = new ProcessIdentifierType();
        processIdentifier.setUuid(responseKey);

        EntityIdentifierType entityIdentifier = new EntityIdentifierType();
        entityIdentifier.setEntity(Constants.IPAGOO_ENTITY);
        entityIdentifier.setBrand(Constants.IPAGOO_BRAND);

        // Create the correspondent event
        Event event = new Event();
        event.setEvent(eventType);
        event.setProcessIdentifier(processIdentifier);
        event.setEntityIdentifier(entityIdentifier);

        LOG.debug("Response event generated correctly: {}", eventData);

        return event;
    }
}
