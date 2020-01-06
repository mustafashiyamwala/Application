package com.org.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Configuration
public class MessageProducer {

	@Autowired
	private KafkaTemplate<String, byte[]> kafkaTemplate;

	public void sendMessage(String topicName, String key, byte[] message) {
		ListenableFuture<SendResult<String, byte[]>> listenableFuture = kafkaTemplate.send(topicName, key, message);
		listenableFuture.addCallback(new ListenableFutureCallback<SendResult<String, byte[]>>() {

			@Override
			public void onSuccess(SendResult<String, byte[]> result) {
				// TODO Auto-generated method stub
				log.info("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
			}

			@Override
			public void onFailure(Throwable ex) {
				// TODO Auto-generated method stub
				log.warn("Unable to send message=[" + message + "] due to : " + ex.getMessage());
			}
		});
	}

}
