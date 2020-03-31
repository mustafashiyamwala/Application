package com.org.stream.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class WrapperSerde<T> implements Serde<T> {

	private Serializer<T> serializer;
	private Deserializer<T> deserializer;

	public WrapperSerde(Serializer<T> serializer, Deserializer<T> deserializer) {
		this.serializer = serializer;
		this.deserializer = deserializer;
	}

	@Override
	public Serializer<T> serializer() {
		// TODO Auto-generated method stub
		return serializer;
	}

	@Override
	public Deserializer<T> deserializer() {
		// TODO Auto-generated method stub
		return deserializer;
	}
}
