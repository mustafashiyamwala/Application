package com.org.producer;

import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

public class CustomPartitioner implements Partitioner {

	private String skewPartition;

	// Configure this class with the given key-value pairs
	public void configure(Map<String, ?> configs) {
		// TODO Auto-generated method stub
		this.skewPartition = configs.get("skew.partition.name").toString();
	}

	// partitionsForTopic: Get the list of partitions for this topic.
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		// TODO Auto-generated method stub
		List<PartitionInfo> partitionInfo = cluster.partitionsForTopic(topic);
		int numPartition = partitionInfo.size();

		if ((keyBytes == null) && (!(key instanceof String))) {
			throw new InvalidRecordException("We expect all messages to have customer name as key");
		}

		if (key.equals(this.skewPartition)) {
			return numPartition-1;
		}
		
		return Math.abs(Utils.murmur2(keyBytes)) % (numPartition - 1);
	}

	// This is called when partitioner is closed.
	public void close() {
		// TODO Auto-generated method stub

	}
}
