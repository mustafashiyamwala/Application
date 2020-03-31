package com.org.stream.join;

import com.org.stream.serde.JsonDeserializer;
import com.org.stream.serde.JsonSerializer;
import com.org.stream.serde.WrapperSerde;

public class SearchSerde extends WrapperSerde<Search> {

	public SearchSerde() {
		super(new JsonSerializer<Search>(), new JsonDeserializer<Search>(Search.class));
		// TODO Auto-generated constructor stub
	}
}
