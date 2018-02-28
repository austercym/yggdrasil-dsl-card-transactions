package com.orwellg.yggdrasil.dsl.card.transactions.common.bolts;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.yggdrasil.dsl.card.transactions.services.CardMessageMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;

import java.util.Arrays;
import java.util.Map;

public class EventToTransactionInfoBolt extends GenericEventProcessBolt<Message> {

	private static final long serialVersionUID = 1L;

	private final static Logger LOG = LogManager.getLogger(EventToTransactionInfoBolt.class);

	private CardMessageMapper mapper;

	public EventToTransactionInfoBolt() {
		super(Message.class);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		mapper = new CardMessageMapper();
	}

	@Override
	protected Object process(Message eventData, String key, String processId) {
		return mapper.map(eventData);
	}

	@Override
	public void declareFieldsDefinition() {
		addFielsDefinition(Arrays.asList(Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA));
	}
}
