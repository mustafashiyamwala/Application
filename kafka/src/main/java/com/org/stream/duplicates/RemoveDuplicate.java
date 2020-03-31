package com.org.stream.duplicates;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
//import org.apache.kafka.streams.state.QueryableStoreTypes;
//import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

public class RemoveDuplicate {

	private Properties properties;

	public RemoveDuplicate() {
		configuringStreaming();
	}

	private void configuringStreaming() {
		this.properties = new Properties();
		this.properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-duplication");
		this.properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		this.properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		this.properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		this.properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
		this.properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
		this.properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "1024");
		// this.properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, "all");
		this.properties.put(StreamsConfig.STATE_DIR_CONFIG, "src/main/resources");

		this.properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	}

	public void createStreams(StreamsBuilder streamBuilder, String sorceTopicName, String sinkTopicName,
			String storeName) {

		Consumed<String, String> consumer = Consumed.with(Serdes.String(), Serdes.String());
		KStream<String, String> kStream = streamBuilder.stream(sorceTopicName, consumer);

		// Transform each record of the input stream into zero or one record in the
		// output stream (both key and value typecan be altered arbitrarily).
		// A Transformer (provided by the given TransformerSupplier) is applied to each
		// input record and returns zero or one output record.
		// Thus, an input record <K,V> can be transformed into an output record <K':V'>.
		// This is a stateful record-by-record operation (cf. map()).
		// Further more, via Punctuator#punctuate(),the processing progress can be
		// observed and additional periodic actions can be performed.
		// In order to assign a state, the state must be created and registered before
		// hand (it's not required to connect global state stores; read-only access to
		// global state stores is available by default)

		// Even if any upstream operation was key-changing, no auto-repartition is
		// triggered.
		// If repartitioning is required, a call to through() should be performed before
		// transform().
		// Transforming records might result in an internal data redistribution, if a
		// key based operator (like an aggregation or join) is applied to the result
		// KStream.(cf. transformValues() )

		// Note that it is possible to emit multiple records for each input record by
		// using context#forward() in Transformer#transform() and
		// Punctuator#punctuate().
		// Be aware that a mismatch between the types of the emitted records and the
		// type of the stream would only be detected at runtime.
		// To ensure type-safety at compile-time, context#forward() should not be used
		// in Transformer#transform() and Punctuator#punctuate().
		// If in Transformer#transform() multiple records need to be emitted for each
		// input record, it is recommended to use flatTransform().
		// StoreNames - the names of the state stores used by the processor.

		// A TransformerSupplier interface which can create one or more Transformer
		// instances.

		// A key-value pair defined for a single Kafka Streams record.
		// If the record comes directly from a Kafka topic then its key/value are
		// defined as the message key/value.
		KStream<String, String> kStream1 = kStream
				.transform(new TransformerSupplier<String, String, KeyValue<String, String>>() {

					@Override
					public Transformer<String, String, KeyValue<String, String>> get() {
						// TODO Auto-generated method stub
						return new removeDuplicateUsingTransform(storeName);
					}
				}, storeName);

		kStream1.to(sinkTopicName);

	}

	public static void main(String[] args) {

		String storeName = "eventId-store";
		StreamsBuilder streamBuilder = new StreamsBuilder();

		// A store supplier that can be used to create one or more KeyValueStore<Byte,
		// byte[]> instances of type <Byte, byte[]>.
		// For any stores implementing the KeyValueStore<Byte, byte[]> interface, null
		// value bytes are considered as "not exist".
		// This means:1. Null value bytes in put operations should be treated as delete.
		// 2. If the key does not exist, get operations should return null value bytes.

		// Factory for creating custom state stores in Kafka Streams.
		// When using the high-level DSL, i.e., StreamsBuilder, users create
		// StoreSuppliers that can be further customized via Materialized.
		// For example, a topic read as KTable can be materialized into an in-memory
		// store with custom key/value serdes and caching disabled.
		// When using the Processor API, i.e., Topology, users create StoreBuilders that
		// can be attached to Processors.
		// For example, you can create a windowed RocksDB store with custom changelog
		// topic configuration.

		// Create a persistent KeyValueBytesStoreSupplier.
		// This store supplier can be passed into a
		// keyValueStoreBuilder(KeyValueBytesStoreSupplier, Serde, Serde).
		// If you want to create a TimestampedKeyValueStore you should use
		// persistentTimestampedKeyValueStore(String) to create a store supplier
		// instead.
		KeyValueBytesStoreSupplier keyValueBytesStoreSupplier = Stores.persistentKeyValueStore(storeName);

		// Build a StateStore wrapped with optional caching and logging.

		// A key-value store that supports put/get/delete and range queries.

		// Creates a StoreBuilder that can be used to build a KeyValueStore.
		// The provided supplier should not be a supplier for TimestampedKeyValueStores.
		StoreBuilder<KeyValueStore<String, Long>> storeBuilder = Stores.keyValueStoreBuilder(keyValueBytesStoreSupplier,
				Serdes.String(), Serdes.Long());

		// Adds a custom state store to the underlying Topology.
		// It is required to connect custom state stores to Processors, before
		// Transformers, or ValueTransformers can used
		streamBuilder.addStateStore(storeBuilder);

		RemoveDuplicate removeDuplicateStreaming = new RemoveDuplicate();
		removeDuplicateStreaming.createStreams(streamBuilder, "with-duplicates", "without-duplicates", storeName);

		Topology topology = streamBuilder.build();

		final KafkaStreams kafkaStreams = new KafkaStreams(topology, removeDuplicateStreaming.properties);

		// A key-value store that only supports read operations.
		// Implementations should be thread-safe as concurrent reads and writes are
		// expected.
		// Please note that this instance defines the thread-safe read functionality
		// only; it does not guarantee anything about whether the actual instance is
		// writable by another thread, or whether it uses some locking mechanism under
		// the hood.
		// For this reason, making dependencies between the read and write operations on
		// different StateStore instances can cause concurrency problems like deadlock.

		// Get a facade wrapping the local StateStore instances with the provided
		// storeName
		// if the Store's type is accepted by the provided QueryableStoreTypes.
		// The returned object can be used to query the local StateStore instances.

		// Provides access to the QueryableStoreTypes provided with KafkaStreams.
		// These can be used with KafkaStreams.store(String, QueryableStoreType).
		// To access and query the StateStores that are part of a Topology/Processor.

		/*
		 * ReadOnlyKeyValueStore<String, Long> keyValueStore =
		 * kafkaStreams.store("eventId-store", QueryableStoreTypes.<String,
		 * Long>keyValueStore());
		 * 
		 * Long valueForKey = keyValueStore.get("mustafa");
		 * System.out.println("Count = " + valueForKey);
		 */

		final CountDownLatch latch = new CountDownLatch(1);
		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
			@Override
			public void run() {
				kafkaStreams.close();
				latch.countDown();
			}
		});

		try {
			kafkaStreams.start();
			latch.await();

		} catch (final Throwable e) {
			System.exit(1);
		}

		System.exit(0);
	}

	// The Transformer interface is for stateful mapping of an input record to zero,
	// one, or multiple new output records (both key and value type can be altered
	// arbitrarily).
	// This is a stateful record-by-record operation, i.e, transform(Object, Object)
	// is invoked individually for each record of a stream and can access and modify
	// a state that is available beyond a single call of transform(Object, Object)
	// (cf. KeyValueMapper for stateless record transformation).
	// Additionally, this Transformer can schedule a method to be called
	// periodically with the provided context.
	// Use TransformerSupplier to provide new instances of Transformer to Kafka
	// Stream's runtime.
	// If only a record's value should be modified ValueTransformer can be used.
	class removeDuplicateUsingTransform implements Transformer<String, String, KeyValue<String, String>> {

		private String storeName;
		private StateStore stateStore;

		// Processor context interface.
		private ProcessorContext processorContext;
		private KeyValueStore<String, Long> keyValueStore;

		private long maintainDurationMs = Duration.ofMillis(5000).toMillis();

		public removeDuplicateUsingTransform(String storeName) {
			// TODO Auto-generated constructor stub
			this.storeName = storeName;
		}

		// Initialize this transformer.
		// This is called once per instance when the topology gets initialized.
		// When the framework is done with the transformation, close() will be called on
		// it; the framework may later re-use the Transformer by calling
		// init(ProcessorContext) again.
		// The provided context can be used to access topology/DSL and current record
		// meta data, to schedule a method to be called periodically and to access
		// attached StateStores.
		// Note, that ProcessorContext is updated in the background with the current
		// record's meta data.
		// Thus, it only contains valid record meta data when accessed within
		// transform(Object, Object).
		@SuppressWarnings("unchecked")
		@Override
		public void init(ProcessorContext processorContext) {
			// TODO Auto-generated method stub
			this.processorContext = processorContext;

			// Get the state store given the store name.
			this.stateStore = this.processorContext.getStateStore(this.storeName);
			this.keyValueStore = (KeyValueStore<String, Long>) this.stateStore;

			System.out.println("Here");

			// A functional interface used as an argument to
			// ProcessorContext.schedule(Duration, PunctuationType, Punctuator).
			// Perform the scheduled periodic operation.
			// The operation is being called, depending on PunctuationType
			this.processorContext.schedule(Duration.ofMillis(1000), PunctuationType.STREAM_TIME, new Punctuator() {

				@Override
				public void punctuate(long currentStreamTimeMs) {
					// TODO Auto-generated method stub
					System.out.println("Scheduler: " + currentStreamTimeMs);
					purgeExpiredEvents(currentStreamTimeMs);
				}
			});
		}

		// Transform the record with the given key and value.
		// Additionally, custom state store that is attached to this operator can be
		// accessed and modified arbitrarily (cf.
		// ProcessorContext.getStateStore(String)).
		// Zero or one record will be forwarded to downstream, transform can return a
		// new KeyValue.
		// If more than one output record should be forwarded to downstream,
		// ProcessorContext.forward(Object, Object) and ProcessorContext.forward(Object,
		// Object, To) can be used.
		// If no record should be forwarded to downstream, transform can return null.
		// Note that returning a new KeyValue pair is merely for convenience.
		// The same can be achieved by using ProcessorContext.forward(Object, Object)
		// and returning null.
		@Override
		public KeyValue<String, String> transform(String key, String value) {
			// TODO Auto-generated method stub
			String event = value;

			if (event != null) {
				// Get the value corresponding to this key.
				if (this.keyValueStore.get(event) != null) {
					System.out.println("Already Exists");
					return null;

				} else {
					// Update the value associated with this key.
					// The value to update, it can be null; if the serialized bytes are also null it
					// is interpreted as deletes.
					System.out.println("Add new Record: " + this.processorContext.timestamp());
					this.keyValueStore.put(event, this.processorContext.timestamp());

					// Create a new key-value pair.
					return KeyValue.<String, String>pair(key, value);
				}
			}
			return KeyValue.<String, String>pair(key, value);
		}

		// Close this transformer and clean up any resources.
		// The framework may later re-use this transformer operator by calling
		// init(ProcessorContext) on it again.
		@Override
		public void close() {
			// TODO Auto-generated method stub

		}

		private void purgeExpiredEvents(long currentStreamTimeMs) {
			// Iterator interface of KeyValue.
			// Users must call its close method explicitly upon completeness to release
			// resources, or use try-with-resources statement (available since JDK7) for
			// this Closeable class.

			// Return an iterator over all keys in this custom state store.
			// This iterator must be closed after use.
			// The returned iterator must be safe from ConcurrentModificationExceptions and
			// must not return null values.
			// No ordering guarantees are provided.
			try (KeyValueIterator<String, Long> keyValueIterator = this.keyValueStore.all()) {

				while (keyValueIterator.hasNext()) {
					// A key-value pair defined for a single Kafka Streams record.
					// If the record comes directly from a Kafka topic then its key/value are
					// defined as the message key/value
					KeyValue<String, Long> keyValue = keyValueIterator.next();

					// The value of the key-value pair.
					long eventTimestampMs = keyValue.value;

					if ((currentStreamTimeMs - eventTimestampMs) > maintainDurationMs) {

						// Delete the value from the state store (if there is one).

						// The key of the key-value pair.
						System.out.println("Remove old records: " + eventTimestampMs);
						this.keyValueStore.delete(keyValue.key);
					}

				}
			}
		}
	}
}
