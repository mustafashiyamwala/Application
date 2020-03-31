package com.org.producer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import com.org.custom.serde.Customer;
import com.org.custom.serde.CustomerSerializer;
import lombok.Data;

@Data
public class Producer implements Closeable {

	private Properties properties;
	private KafkaProducer<String, Customer> kafkaProducer;
	private ProducerRecord<String, Customer> record;

	public Producer() {
		configuringProducer();
		connectionProducer();
	}

	public void configuringProducer() {
		this.properties = new Properties();
		this.properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		this.properties.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
		this.properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		this.properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomerSerializer.class);
		this.properties.put(ProducerConfig.ACKS_CONFIG, "0");
		this.properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "100");
		this.properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "1000");
		this.properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
		this.properties.put(ProducerConfig.RETRIES_CONFIG, "5");
		this.properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, "200");
		this.properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.org.producer.CustomPartitioner");
		this.properties.put("skew.partition.name", "mustafa");

		/*
		 * this.properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		 * this.properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,
		 * "transaction-records");
		 */
	}

	public void connectionProducer() {
		this.kafkaProducer = new KafkaProducer<String, Customer>(this.properties);
	}

	// Fire and Forgot
	public void sendMessage(String topicName, String key, Customer value) {
		this.record = new ProducerRecord<String, Customer>(topicName, key, value);
		// Asynchronously send a record to a topic.
		this.kafkaProducer.send(record);
	}

	// Synchronous
	public RecordMetadata sendMessage1(String topicName, String key, Customer value) throws Exception {

		try {
			this.record = new ProducerRecord<String, Customer>(topicName, key, value);
			Future<RecordMetadata> future = this.kafkaProducer.send(record, null);
			RecordMetadata recordMetadata = future.get();
			return recordMetadata;

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			throw new Exception(e.getMessage());

		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			throw new Exception(e.getMessage());
		}
	}

	// Asynchronous
	public void sendMessage2(String topicName, String key, Customer value) throws Exception {

		this.record = new ProducerRecord<String, Customer>(topicName, key, value);
		this.kafkaProducer.send(record, new AsyncProducer());
	}

	// Transactional
	public void producer(String topicName) {

		// this.kafkaProducer.initTransactions();
		try {
			this.kafkaProducer.beginTransaction();

			for (int i = 0; i < 100; i++) {
				this.kafkaProducer.send(new ProducerRecord(topicName, Integer.toString(i), Integer.toString(i)));
			}

			// this.kafkaProducer.commitTransaction();

		} catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
			// We can't recover from these exceptions, so our only option is to close the
			// producer and exit.
			this.kafkaProducer.close();

		} catch (KafkaException e) {
			// For all other exceptions, just abort the transaction and try again.
			// this.kafkaProducer.abortTransaction();

		}
	}

	public void close() throws IOException {
		// This method blocks until all previously sent requests complete.
		this.kafkaProducer.close();
	}

}
