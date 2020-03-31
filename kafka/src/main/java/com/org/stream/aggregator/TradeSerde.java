package com.org.stream.aggregator;

import com.org.stream.serde.JsonDeserializer;
import com.org.stream.serde.JsonSerializer;
import com.org.stream.serde.WrapperSerde;

public final class TradeSerde extends WrapperSerde<Trade> {

	public TradeSerde() {
		super(new JsonSerializer<Trade>(), new JsonDeserializer<Trade>(Trade.class));
		// TODO Auto-generated constructor stub
	}

}
