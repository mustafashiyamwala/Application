package com.org.consumer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

public class PartitionRebalance implements ConsumerRebalanceListener {

	private Consumer consumer;
	private Map<TopicPartition, Long> dbMap;

	public PartitionRebalance(Consumer consumer) {
		this.consumer = consumer;
		this.dbMap = new HashMap<TopicPartition, Long>();
	}

	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		// TODO Auto-generated method stub
		for (TopicPartition topicPartition : partitions) {

			TopicPartition topicPartition2 = new TopicPartition(topicPartition.topic(), topicPartition.partition());
			Long offset = this.consumer.getTopicPartitionOffset().get(topicPartition2).offset() + 1;

			
			
			this.dbMap.put(topicPartition2, offset);

			System.out.println("Lost partitions in rebalance. Committing current offsets: "
					+ this.consumer.getTopicPartitionOffset());
		}
	}

	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		// TODO Auto-generated method stub

		for (TopicPartition topicPartition : partitions) {
			Long offset = this.dbMap.get(topicPartition);
			this.consumer.getKafkaConsumer().seek(topicPartition, offset);

			System.out.println("Topic Partition: " + topicPartition.partition());
		}

		// this.consumer.getKafkaConsumer().seekToBeginning(partitions);
		// this.consumer.getKafkaConsumer().seekToEnd(partitions);
	}
}
