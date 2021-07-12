package com.org.main;

import java.util.Arrays;
import org.apache.kafka.clients.producer.RecordMetadata;
import com.org.consumer.Consumer;
import com.org.custom.serde.Customer;
import com.org.producer.Producer;

public class MainApp {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		int numThread = 2;
		String topicName = "KafkaMessage";
		Customer customer = new Customer(1, "Jou");

		Producer producer = new Producer();
		producer.sendMessage(topicName, "1", customer);
		Thread.sleep(100);
		

		/*RecordMetadata recordMetadata = producer.sendMessage1(topicName, "mustafa", customer);
		System.out.println("Topic Name: " + recordMetadata.topic() + " Partition: " + recordMetadata.partition()
				+ " Offset: " + recordMetadata.offset() + " Event Time: " + recordMetadata.timestamp());
		Thread.sleep(100);
		
		producer.sendMessage2(topicName, "4", customer);
		System.out.println("Done");*/

		
		/*for (int i = 0; i < numThread; i++) {
			Consumer consumer = new Consumer(Arrays.asList(topicName));
			Thread thread = new Thread(consumer, "Thread-"+i);
			thread.start();
		}	*/	
	}
}
