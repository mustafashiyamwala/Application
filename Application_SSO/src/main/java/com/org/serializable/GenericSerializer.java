package com.org.serializable;

import java.util.Map;
import org.apache.kafka.common.serialization.Serializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.org.dto.GenericObjectDto;
import lombok.extern.log4j.Log4j2;

@Log4j2
public class GenericSerializer implements Serializer<GenericObjectDto> {

	public void configure(Map<String, ?> configs, boolean isKey) {
		// TODO Auto-generated method stub
	}

	@Override
	public byte[] serialize(String topic, GenericObjectDto data) {
		// TODO Auto-generated method stub
		byte[] serializeValue = null;
		ObjectMapper objectMapper = new ObjectMapper();

		try {
			serializeValue = objectMapper.writeValueAsBytes(data);

		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			log.error("Unable to Serialize " + e.getMessage());
		}

		return serializeValue;
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}
	
}
