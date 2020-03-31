package com.org.stream.join;

import com.org.stream.serde.JsonDeserializer;
import com.org.stream.serde.JsonSerializer;
import com.org.stream.serde.WrapperSerde;

public class PageViewSerde extends WrapperSerde<PageView> {

	public PageViewSerde() {
		super(new JsonSerializer<PageView>(), new JsonDeserializer<PageView>(PageView.class));
		// TODO Auto-generated constructor stub
	}
}
