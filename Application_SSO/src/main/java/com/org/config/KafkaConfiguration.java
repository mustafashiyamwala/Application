package com.org.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import lombok.extern.log4j.Log4j2;

@Log4j2
@Configuration
public class KafkaConfiguration {

	@Value(value = "${kafka.bootstrapAddress}")
	private String kafkaAddress;

	@Value(value = "${kafka.groupId}")
	private String groupId;

	@Value(value = "${kafka.topicName}")
	private String topic;

	@Value(value = "${kafka.noPartition}")
	private Integer noPartition;

	@Value(value = "${kafka.replicationFactor}")
	private Integer replicationFactor;

	public KafkaAdmin kafkaAdmin() {
		Map<String, Object> config = new HashMap<String, Object>();
		config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaAddress);
		return new KafkaAdmin(config);
	}

	public NewTopic topic() {
		return new NewTopic(this.topic, this.noPartition, this.replicationFactor.shortValue());
	}

	public ProducerFactory<String, String> producerFactory() {
		Map<String, Object> config = new HashMap<String, Object>();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaAddress);
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		return new DefaultKafkaProducerFactory<String, String>(config);
	}

	public KafkaTemplate<String, String> kafkaTemplate() {
		log.info("Kafka Producer Connection is Done");
		return new KafkaTemplate<String, String>(producerFactory());
	}

	public ConsumerFactory<String, String> consumerFactory() {
		Map<String, Object> config = new HashMap<String, Object>();
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.kafkaAddress);
		config.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		return new DefaultKafkaConsumerFactory<String, String>(config);
	}

	public ConcurrentKafkaListenerContainerFactory<String, String> concurrentKafkaListenerContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory());
		factory.setRecordFilterStrategy(record -> record.key().contains(""));
		
		log.info("Kafka Consumer Connection is Done");
		return factory;
	}
}
