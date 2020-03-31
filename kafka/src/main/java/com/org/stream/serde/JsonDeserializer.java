package com.org.stream.serde;

import org.apache.kafka.common.serialization.Deserializer;
import com.google.gson.Gson;

public class JsonDeserializer<T> implements Deserializer<T> {

	private Gson gson = new Gson();

	private Class<T> deserializedClass;

	public JsonDeserializer(Class<T> deserializedClass) {
		this.deserializedClass = deserializedClass;
	}

	@Override
	public T deserialize(String topic, byte[] data) {
		// TODO Auto-generated method stub
		if (data == null) {
			return null;
		}

		return gson.fromJson(new String(data), deserializedClass);
	}

}
