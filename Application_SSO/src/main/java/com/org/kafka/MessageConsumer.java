package com.org.kafka;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

public class MessageConsumer {

	/*
	 * @KafkaListener(topicPartitions = @TopicPartition(topic = "Apps",
	 * partitionOffsets = {
	 * 
	 * @PartitionOffset(partition = "0", initialOffset = "0"),
	 * 
	 * @PartitionOffset(partition = "3", initialOffset = "0") }))
	 */

	/*
	 * @KafkaListener(topicPartitions = @TopicPartition(topic = "Apps", partition =
	 * {"0", "1"}))
	 */

	@KafkaListener(topics = "Apps", groupId = "SSO", clientIdPrefix = "bytearray", containerFactory = "kafkaListenerByteArrayContainerFactory")
	public void consumeMessage(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
		System.out.println("Received Message: " + message + "from partition: " + partition);
	}

}
