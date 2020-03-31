package com.org.custom.serde;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import org.apache.kafka.common.serialization.Serializer;
import lombok.NoArgsConstructor;

@NoArgsConstructor
public class CustomerSerializer implements Serializer<Customer> {

	public byte[] serialize(String topic, Customer data) {
		// TODO Auto-generated method stub

		int serializeNameSize = 0;
		byte[] serializeName = null;

		try {
			if (data == null) {
				return null;

			} else {

				if (data.getCustomerName() != null) {
					serializeName = data.getCustomerName().getBytes("UTF-8");
					serializeNameSize = serializeName.length;

				} else {
					serializeName = new byte[0];
					serializeNameSize = 0;
				}
			}
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			System.out.println("Unable to Serialize: " + e.getMessage());
		}

		ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + serializeNameSize);
		byteBuffer.putInt(data.getCustomerID());
		byteBuffer.putInt(serializeNameSize);
		byteBuffer.put(serializeName);

		return byteBuffer.array();
	}	
}
