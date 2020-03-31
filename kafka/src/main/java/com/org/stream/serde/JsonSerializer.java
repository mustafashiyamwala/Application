package com.org.stream.serde;

import java.nio.charset.Charset;
import org.apache.kafka.common.serialization.Serializer;
import com.google.gson.Gson;

public class JsonSerializer<T> implements Serializer<T> {

	private Gson gson = new Gson();

	@Override
	public byte[] serialize(String topic, T data) {
		// TODO Auto-generated method stub
		return gson.toJson(data).getBytes(Charset.forName("UTF-8"));
	}

}
