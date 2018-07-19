package com.orwellg.yggdrasil.dsl.card.transactions.common.bolts;

import com.orwellg.umbrella.avro.types.gps.Message;
import com.orwellg.yggdrasil.card.transaction.commons.MapperFactory;
import com.orwellg.yggdrasil.card.transaction.commons.bolts.Fields;
import com.orwellg.yggdrasil.card.transaction.commons.bolts.GenericEventMappingBolt;
import com.orwellg.yggdrasil.card.transaction.commons.model.TransactionInfo;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.modelmapper.ModelMapper;

import java.util.Arrays;
import java.util.Map;

public class EventToTransactionInfoBolt extends GenericEventMappingBolt<Message> {

	private static final long serialVersionUID = 1L;

	private ModelMapper mapper;

	public EventToTransactionInfoBolt() {
		super(Message.class);
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		super.prepare(stormConf, context, collector);
		mapper = MapperFactory.getMapper();
	}

	@Override
	protected Object mapEvent(Message eventData, String key, String processId) {
		return mapper.map(eventData, TransactionInfo.class);
	}

	@Override
	public void declareFieldsDefinition() {
		addFielsDefinition(Arrays.asList(Fields.KEY, Fields.PROCESS_ID, Fields.EVENT_DATA));
	}
}
