package com.org.stream.aggregator;

import com.org.stream.serde.JsonDeserializer;
import com.org.stream.serde.JsonSerializer;
import com.org.stream.serde.WrapperSerde;

public class TradeStatisticsSerde extends WrapperSerde<TradeStatistics> {

	public TradeStatisticsSerde() {
		super(new JsonSerializer<TradeStatistics>(), new JsonDeserializer<TradeStatistics>(TradeStatistics.class));
		// TODO Auto-generated constructor stub
	}

}
