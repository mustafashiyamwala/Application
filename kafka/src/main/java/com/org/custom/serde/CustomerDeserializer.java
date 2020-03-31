package com.org.custom.serde;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class CustomerDeserializer implements Deserializer<Customer> {

	public Customer deserialize(String topic, byte[] data) {
		// TODO Auto-generated method stub
		int customerID = 0;
		String customerName = null;

		if (data == null)
			return null;

		if (data.length < 8)
			throw new SerializationException("Size of data received by IntegerDeserializer is shorter than expected");

		try {
			ByteBuffer byteBuffer = ByteBuffer.wrap(data);
			customerID = byteBuffer.getInt();
			int nameSize = byteBuffer.getInt();

			byte[] name = new byte[nameSize];
			byteBuffer.get(name);

			customerName = new String(name, "UTF-8");

		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			System.out.println("Deserialization :" + e.getMessage());
		}

		return new Customer(customerID, customerName);
	}
}
