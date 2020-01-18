package com.org.serializable;

import java.io.IOException;
import java.util.Map;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Component
public class GenericDeserializer<T> implements Deserializer<T> {

	private Class<?> t;

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub

	}

	public T deserialize(String topic, byte[] data, Class<?> t) {
		this.t = t;
		return deserialize(topic, data);
	}

	@SuppressWarnings("unchecked")
	@Override
	public T deserialize(String topic, byte[] data) {
		// TODO Auto-generated method stub

		T genericObjectDto = null;
		ObjectMapper objectMapper = new ObjectMapper();

		try {
			genericObjectDto = (T) objectMapper.readValue(data, this.t);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			log.error("Unable to De-Serialize " + e.getMessage());
		}

		return genericObjectDto;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub

	}
}
