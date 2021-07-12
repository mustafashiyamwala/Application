package com.org.main;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IngestingMessage {

	private static String TOPIC_NAME;// = "ingress.conti.vehicledata.string";
	private static String FILE_PATH; // = "src/main/resources/configuration.properties";

	public static Properties configuration() throws Exception {
		Properties properties = new Properties();
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "Kafka-EventHub");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

		try {
			properties.load(new FileReader(FILE_PATH));
			TOPIC_NAME = properties.getProperty("topic.name");

		} catch (IOException e) {
			throw new Exception("Unable to Find the File " + FILE_PATH, e);
		}

		return properties;
	}

	public static void main(String[] args) {

		FILE_PATH = args[0];
		IngestingMessage producer = new IngestingMessage();

		try {

			Stream<String> stream = Files.lines(Paths.get(args[1]));
			KafkaProducer<String, String> kafkaProducer = producer.createConnection();

			stream.forEach(message -> {
				try {
					producer.publishMessage(kafkaProducer, "5", message);
					Thread.sleep(1000);

				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					System.out.println("Exception: " + e.getMessage());
				}
			});

			kafkaProducer.close();

		} catch (Exception e) {
			System.out.println("Exception: " + e.getMessage());

		}
	}

	public KafkaProducer<String, String> createConnection() throws Exception {
		Properties properties = IngestingMessage.configuration();

		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
		return kafkaProducer;
	}

	public void publishMessage(KafkaProducer<String, String> kafkaProducer, String key, String message) {
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(TOPIC_NAME, key, message);

		kafkaProducer.send(producerRecord, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (exception != null) {
					System.out.println("Exception: " + exception.getMessage());

				} else {
					System.out.println("Record Metadata: " + metadata.toString());
				}
			}
		});
	}
}
