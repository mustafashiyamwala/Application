package com.org.consumer;

import java.sql.ResultSet;
import java.sql.SQLException;
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

			String sql = "REPLACE INTO OFFSETS VALUES(" + topicPartition.topic() + "," + topicPartition.partition() + ","
					+ this.consumer.getTopicPartitionOffset().get(topicPartition2).offset() + 1;
			this.consumer.getSqlStorage().pushRecords(sql);

			this.dbMap.put(topicPartition2, offset);

			System.out.println("Lost partitions in rebalance. Committing current offsets: "
					+ this.consumer.getTopicPartitionOffset());
		}
	}

	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		// TODO Auto-generated method stub

		try {
			for (TopicPartition topicPartition : partitions) {
				String sql = "SELECT OFFSETID FROM OFFSETS WHERE topic=" + topicPartition.topic() + " AND PARTITIONID="
						+ topicPartition.partition();

				ResultSet resultSet = this.consumer.getSqlStorage().pullRecords(sql);
				Long offset = resultSet.getLong(1);

				this.consumer.getKafkaConsumer().seek(topicPartition, offset);
				System.out.println("Topic Partition: " + topicPartition.partition());
			}

			// this.consumer.getKafkaConsumer().seekToBeginning(partitions);
			// this.consumer.getKafkaConsumer().seekToEnd(partitions);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("Exception: " + e.getMessage());
		}
	}
}
