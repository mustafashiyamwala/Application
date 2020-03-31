package com.org.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

public class AsyncProducer implements Callback {

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		// TODO Auto-generated method stub
		if (exception != null) {
			System.out.println(exception.getMessage());
		}
	}
}
