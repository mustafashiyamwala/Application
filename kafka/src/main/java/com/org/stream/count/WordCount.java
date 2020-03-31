package com.org.stream.count;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.Topology.AutoOffsetReset;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;

public class WordCount {

	private Properties properties;

	public WordCount() {
		configuringStreaming();
	}

	private void configuringStreaming() {
		this.properties = new Properties();
		this.properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
		this.properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		this.properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		this.properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		this.properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");
		this.properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
		this.properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "1024");
		// this.properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, "all");

		this.properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	}

	@SuppressWarnings("static-access")
	private void createStreams(StreamsBuilder streamBuilder, String sorceTopicName, String sinkTopicName) {

		// The Consumed class is used to define the optional parameters when using
		// StreamsBuilder to build instances of KStream, KTable, and GlobalKTable.
		Consumed<String, String> consumer = Consumed.with(Serdes.String(), Serdes.String())
				.with(AutoOffsetReset.EARLIEST);

		// KStream is an abstraction of a record stream of KeyValue pairs, i.e., each
		// record is an independent entity/event in the real world.
		// For example a user X might buy two items I1 and I2, and thus there might be
		// two records <K:I1>, <K:I2>in the stream.
		// A KStream is either defined from one or multiple Kafka topics that are
		// consumed message by message or the result of a KStream transformation.
		// A KTable can also be converted into a KStream.
		// A KStream can be transformed record by record, joined with another KStream,
		// KTable, GlobalKTable, or can be aggregated into a KTable.
		// Kafka Streams DSL can be mixed-and-matched with Processor API (PAPI) (c.f.
		// Topology) via process(...), transform(...), and transformValues(...).

		// Create a KStream from the specified topic.
		// The "auto.offset.reset" strategy, TimestampExtractor, key and value
		// deserializers are defined by the options in Consumed are used.
		// Note that the specified input topic must be partitioned by key.
		// If this is not the case it is the user's responsibility to repartition the
		// data before any key based operation(like aggregation or join) is applied to
		// the returned KStream.
		KStream<String, String> kStream = streamBuilder.stream(sorceTopicName, consumer);

		// Print the records of this KStream using the options provided by PrintedNote
		// that this is mainly for debugging/testing purposes, and it will try to flush
		// on each record print.
		// It SHOULD NOT be used for production usage if performance requirements are
		// concerned.
		kStream.print(Printed.toSysOut());

		// Create a new KStream by transforming the value of each record in this stream
		// into zero or more values with the same key in the new stream.
		// This is a stateless record-by-record operation (cf.
		// transformValues(ValueTransformerSupplier, String...)for stateful value
		// transformation).

		// The ValueMapper interface for mapping a value to a new value of arbitrary
		// type.
		// This is a stateless record-by-record operation, i.e, apply(Object) is invoked
		// individually for each recordof a stream (cf. ValueTransformer for stateful
		// value transformation).
		// The provided ValueMapper must return an Iterable (e.g., any Collection
		// type)and the return value must not be null.
		// Thus, no internal data redistribution is required if a key based operator
		// (like an aggregation or join)is applied to the result KStream. (cf.
		// flatMap(KeyValueMapper))
		// KStream that contains more or less records with unmodified keys and new
		// values of different type.
		// If a record's key and value should be modified KeyValueMapper can be used.

		KStream<String, String> kStream1 = kStream.flatMapValues(new ValueMapper<String, Iterable<String>>() {

			public Iterable<String> apply(String value) {
				// TODO Auto-generated method stub
				String words[] = value.toLowerCase().split(" ");
				return Arrays.asList(words);
			}
		});

		kStream1.print(Printed.toSysOut());

		// KGroupedStream is an abstraction of a grouped record stream of KeyValue
		// pairs.
		// It is an intermediate representation of a KStream in order to apply an
		// aggregation operation on the original KStream records.
		// It is an intermediate representation after a grouping of a KStream before an
		// aggregation is applied to the new partitions resulting in a KTable.

		// Group the records of this KStream on a new key that is selected using the
		// provided KeyValueMapper and default serializers and deserializers.
		// Grouping a stream on the record key is required before an aggregation
		// operator can be applied to the data(cf. KGroupedStream).
		// The KeyValueMapper selects a new key (which may or may not be of the same
		// type) while preserving the original values.
		// If the new record key is null the record will not be included in the
		// resulting KGroupedStream
		// Because a new key is selected, an internal repartitioning topic may need to
		// be created in Kafka if a later operator depends on the newly selected key.
		// This topic will be named "${applicationId}-<name>-repartition", where
		// "applicationId" is user-specified in StreamsConfig via parameter
		// APPLICATION_ID_CONFIG,"<name>" is an internally generated name, and
		// "-repartition" is a fixed suffix.
		// You can retrieve all generated internal topic names via Topology.describe().
		// All data of this stream will be redistributed through the repartitioning
		// topic by writing all records to it, and rereading all records from it, such
		// that the resulting KGroupedStream is partitioned on the new key.
		// This operation is equivalent to calling selectKey(KeyValueMapper) followed by
		// groupByKey().
		// If the key type is changed, it is recommended to use groupBy(KeyValueMapper,
		// Grouped) instead.

		// The KeyValueMapper interface for mapping a key-value pair to a new value of
		// arbitrary type.
		// For example, it can be used to map from an input KeyValue pair to an output
		// KeyValue pair with different key and/or value type(for this case output type
		// VR == KeyValue<NewKeyType,NewValueType>)
		// map from an input record to a new key (with arbitrary key type as specified
		// by VR)
		// This is a stateless record-by-record operation, i.e, apply(Object, Object) is
		// invoked individually for each record of a stream (cf. Transformer for
		// stateful record transformation).
		// KeyValueMapper is a generalization of ValueMapper.
		KGroupedStream<String, String> kGroupedStream = kStream1.groupBy(new KeyValueMapper<String, String, String>() {

			public String apply(String key, String value) {
				// TODO Auto-generated method stub
				System.out.println("Key: " + key + " Value: " + value);
				return value;
			}
		});

		// KTable is an abstraction of a changelog stream from a primary-keyed table.
		// Each record in this changelog stream is an update on the primary-keyed table
		// with the record key as the primary key.
		// A KTable is either defined from a single Kafka topic that is consumed message
		// by message or the result of a KTable transformation.
		// An aggregation of a KStream also yields a KTable.
		// A KTable can be transformed record by record, joined with another KTable or
		// KStream, or can be re-partitioned and aggregated into a new KTable.
		// FSome KTables have an internal state (a ReadOnlyKeyValueStore) and are
		// therefore queryable via the interactive queries API.

		// Count the number of records in this stream by the grouped key.
		// Records with null key or value are ignored.
		// The result is written into a local KeyValueStore (which is basically an
		// ever-updating materialized view).
		// Furthermore, updates to the store are sent downstream into a KTable changelog
		// stream.
		// Not all updates might get sent downstream, as an internal cache is used to
		// de-duplicate consecutive updates to the same key.
		// The rate of propagated updates depends on your input data rate, the number of
		// distinct keys, the number of parallel running Kafka Streams instances, and
		// the configuration parameters for cache size, and commit interval.
		// For failure and recovery the store will be backed by an internal changelog
		// topic that will be created in Kafka.
		// The changelog topic will be named
		// "${applicationId}-${internalStoreName}-changelog", where "applicationId"
		// isuser-specified in StreamsConfig via parameter APPLICATION_ID_CONFIG,
		// "internalStoreName" is an internal name and "-changelog" is a fixed suffix.
		// Note that the internal store name may not be queriable through Interactive
		// Queries.
		// You can retrieve all generated internal topic names via Topology.describe().
		KTable<String, Long> kTable = kGroupedStream.count();

		// Convert this changelog stream to a KStream.
		KStream<String, Long> kStream2 = kTable.toStream();

		kStream2.print(Printed.toSysOut());

		// This class is used to provide the optional parameters when producing to new
		// topics using KStream.
		// Create a Produced instance with provided keySerde and valueSerde.
		Produced<String, Long> produced = Produced.with(Serdes.String(), Serdes.Long());

		// Materialize this stream to a topic using the provided Produced instance.
		// The specified topic should be manually created before it is used (i.e.,
		// before the Kafka Streams application is started).
		// This class is used to provide the optional parameters when producing to new
		// topics.
		kStream2.to(sinkTopicName, produced);
	}

	public static void main(String[] args) {
		// StreamsBuilder provide the high-level Kafka Streams DSL to specify a Kafka
		// Streams topology.
		StreamsBuilder streamBuilder = new StreamsBuilder();

		WordCount countStreaming = new WordCount();
		countStreaming.createStreams(streamBuilder, "streams-plaintext-input", "streams-wordcount-output");

		// A logical representation of a Processor Topology.
		// A topology is an acyclic graph of sources, processors, and sinks.
		// A source is a node in the graph that consumes one or more Kafka topics and
		// forwards them to its successor nodes.
		// A processor is a node in the graph that receives input records from upstream
		// nodes, processes the records, and optionally forwarding new records to one or
		// all of its downstream nodes.
		// Finally, a sink is a node in the graph that receives records from upstream
		// nodes and writes them to a Kafka topic.
		// A Topology allows you to construct an acyclic graph of these nodes, and then
		// passed into a new KafkaStreams instance that will then begin consuming,
		// processing, and producing records.
		Topology topology = streamBuilder.build();

		// A Kafka client that allows for performing continuous computation on input
		// coming from one or more input topics and sends output to zero, one, or more
		// output topics.
		// The computational logic can be specified either by using the Topology to
		// define a DAG topology of Processors or by using the StreamsBuilder which
		// provides the high-level DSL to define transformations.
		// One KafkaStreams instance can contain one or more threads specified in the
		// configs for the processing work.
		// A KafkaStreams instance can co-ordinate with any other instances with the
		// same application ID (whether in the same process, on other processes on this
		// machine, or on remote machines) as a single (possibly distributed) stream
		// processing application.
		// These instances will divide up the work based on the assignment of the input
		// topic partitions so that all partitions are being consumed.
		// If instances are added or fail, all (remaining) instances will rebalance the
		// partition assignment among themselvesto balance processing load and ensure
		// that all input topic partitions are processed.
		final KafkaStreams kafkaStreams = new KafkaStreams(topology, countStreaming.properties);

		// retrieve all generated internal topic names.
		topology.describe();

		final CountDownLatch latch = new CountDownLatch(1);

		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
			@Override
			public void run() {
				// Shutdown this KafkaStreams instance by signaling all the threads to stop, and
				// then wait for them to join.
				// This will block until all threads have stopped.
				kafkaStreams.close();
				latch.countDown();
			}
		});

		try {
			// Set the handler, invoked when a internal thread abruptly terminates due to an
			// uncaught exception.
			kafkaStreams.setUncaughtExceptionHandler(
					(Thread thread, Throwable throwable) -> System.out.println(throwable.getMessage()));

			// Start the KafkaStreams instance by starting all its threads.
			// This function is expected to be called only once during the life cycle of the
			// client.
			// Because threads are started in the background, this method does not block.
			// However, if you have global stores in your topology, this method blocks until
			// all global stores are restored.
			// As a consequence, any fatal exception that happens during processing is by
			// default only logged.
			// If you want to be notified about dying threads, you can register an uncaught
			// exception handler before starting the KafkaStreams instance.
			kafkaStreams.start();
			latch.await();

		} catch (final Throwable e) {
			System.exit(1);
		}

		System.exit(0);

	}
}
