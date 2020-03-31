package com.org.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.org.custom.serde.Customer;
import com.org.custom.serde.CustomerDeserializer;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Consumer implements Runnable {

	private Properties properties;
	private List<String> topicsName;
	private KafkaConsumer<String, Customer> kafkaConsumer;
	private Map<TopicPartition, OffsetAndMetadata> map;

	public Consumer(List<String> topicsName) {
		this.topicsName = topicsName;
		this.map = new HashMap<TopicPartition, OffsetAndMetadata>();

		configuringConsumer();
	}

	public Map<TopicPartition, OffsetAndMetadata> getTopicPartitionOffset() {
		return this.map;
	}

	public void configuringConsumer() {
		this.properties = new Properties();
		this.properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		this.properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer");
		this.properties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-1");
		this.properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		this.properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		this.properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		this.properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CustomerDeserializer.class);

		this.properties.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "1000");
		this.properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// this.properties.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "3000");
		this.properties.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");
		this.properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");

		// this.properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
	}

	public void connectionConsumer() {
		this.kafkaConsumer = new KafkaConsumer<String, Customer>(properties);
		// this.kafkaConsumer.subscribe(this.topicsName);
		this.kafkaConsumer.subscribe(this.topicsName, new PartitionRebalance(this));
		// manualpartitionAssignment(this.topicsName);
	}

	public void receiveMessage() {

		int count = 0;
		try {
			while (true) {

				ConsumerRecords<String, Customer> consumerRecords = this.kafkaConsumer.poll(Duration.ofMillis(1000));

				// this.kafkaConsumer.pause(consumerRecords.partitions());
				// this.kafkaConsumer.resume(consumerRecords.partitions());

				for (ConsumerRecord<String, Customer> consumerRecord : consumerRecords) {
					System.out.println("Topic: " + consumerRecord.topic() + " Partition: " + consumerRecord.partition()
							+ " Offset: " + consumerRecord.offset() + " Key: " + consumerRecord.key() + " Value: "
							+ consumerRecord.value() + " Thread: " + Thread.currentThread().getName());

					TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(),
							consumerRecord.partition());
					OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(consumerRecord.offset() + 1,
							"no metadata");
					this.map.put(topicPartition, offsetAndMetadata);

					if (count % 1000 == 0) {
						this.kafkaConsumer.commitAsync(this.map, new OffsetCommitCallback() {

							public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,
									Exception exception) {
								// TODO Auto-generated method stub
								if (exception != null) {
									System.out.println("Commit failed for offsets {}" + offsets + " Excetion:"
											+ exception.getLocalizedMessage());

								}
							}
						});
					}

					count++;
				}
			}

		} finally {
			// this.kafkaConsumer.commitSync();
			this.kafkaConsumer.commitSync(this.map);
			this.kafkaConsumer.close();

		}
	}

	// Manually assigning partition
	public void manualpartitionAssignment(String topicName) {
		int partitionId = 0;
		List<TopicPartition> partitions = new ArrayList<TopicPartition>();

		List<PartitionInfo> partitionInfos = this.kafkaConsumer.partitionsFor(topicName);
		if (partitionInfos != null) {

			for (PartitionInfo partitionInfo : partitionInfos) {

				if (partitionId % 2 == 0) {
					partitions.add(new TopicPartition(partitionInfo.topic(), partitionId));
				}
				partitionId += 1;
			}
			this.kafkaConsumer.assign(partitions);
		}
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		connectionConsumer();
		receiveMessage();
	}
}
