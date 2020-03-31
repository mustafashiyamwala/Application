package com.org.stream.aggregator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import com.org.stream.serde.JsonSerializer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

public class StockGenProducer {

	public static KafkaProducer<String, Trade> producer = null;

	@SuppressWarnings("resource")
	public static void main(String[] args) throws Exception {

		// initialize
		long iter = 0;
		Random random = new Random();
		Map<String, Integer> prices = new HashMap<>();
		Properties props = new Properties();
		JsonSerializer<Trade> tradeSerializer = new JsonSerializer<Trade>();

		// Configuring producer
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "produce");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, tradeSerializer.getClass().getName());

		System.out.println("Press CTRL-C to stop generating data");
		// add shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				System.out.println("Shutting Down");
				if (producer != null)
					producer.close();
			}
		});

		// Starting producer
		producer = new KafkaProducer<>(props);

		for (String ticker : Constants.TICKERS) {
			prices.put(ticker, Constants.START_PRICE);
		}

		// Start generating events, stop when CTRL-C
		while (true) {
			iter++;

			for (String ticker : Constants.TICKERS) {

				int size = random.nextInt(100);
				int price = prices.get(ticker);
				double log = random.nextGaussian() * 0.25 + 1;
				
				// flunctuate price sometimes
				if (iter % 10 == 0) {
					price = price + random.nextInt(Constants.MAX_PRICE_CHANGE * 2) - Constants.MAX_PRICE_CHANGE;
					prices.put(ticker, price);
				}

				Trade trade = new Trade("ASK", ticker, (price + log), size);
				ProducerRecord<String, Trade> record = new ProducerRecord<>(Constants.STOCK_TOPIC, ticker, trade);
				
				producer.send(record, (RecordMetadata r, Exception e) -> {
					if (e != null) {
						System.out.println("Error producing events");
						e.printStackTrace();
					}
				});

				// Sleep a bit, otherwise it is frying my machine
				Thread.sleep(Constants.DELAY);
			}
		}
	}
}
