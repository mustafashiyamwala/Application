package com.org.stream.aggregator;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Aggregator;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindowedKStream;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.state.WindowStore;

public class WordCountStreaming {

	private Properties properties;

	public WordCountStreaming() {
		configuringStreaming();
	}

	private void configuringStreaming() {
		this.properties = new Properties();
		this.properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount");
		this.properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		this.properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		this.properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		this.properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		this.properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "2");
		this.properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
		this.properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "1024");
		// this.properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, "all");
		this.properties.put(StreamsConfig.STATE_DIR_CONFIG, "src/main/resources");

	}

	private void createWindowStreams(StreamsBuilder streamBuilder, String sorceTopicName, String sinkTopicName) {

		Consumed<String, Trade> consumer = Consumed.with(Serdes.String(), new TradeSerde());
		KStream<String, Trade> kStream = streamBuilder.stream(sorceTopicName, consumer);
		kStream.print(Printed.toSysOut());

		// Group the records by their current key into a KGroupedStream while preserving
		// the original values and default serializers and deserializers.
		// Grouping a stream on the record key is required before an aggregation
		// operator can be applied to the data(cf. KGroupedStream).
		// If a record key is null the record will not be included in the resulting
		// KGroupedStream.
		// If a key changing operator was used before this operation (e.g.,
		// selectKey(KeyValueMapper), map(KeyValueMapper), flatMap(KeyValueMapper), or
		// transform(TransformerSupplier, String...)), and no data redistribution
		// happened afterwards (e.g., via through(String)) an internal repartitioning
		// topic may need to be created in Kafka if a later operator depends on the
		// newly selected key.
		// This topic will be named "${applicationId}-<name>-repartition", where
		// "applicationId" is user-specified in StreamsConfig via parameter
		// APPLICATION_ID_CONFIG,"<name>" is an internally generated name, and
		// "-repartition" is a fixed suffix.
		// You can retrieve all generated internal topic names via Topology.describe().
		// For this case, all data of this stream will be redistributed through the
		// repartitioning topic by writing all records to it, and rereading all records
		// from it, such that the resulting KGroupedStream is partitioned correctly on
		// its key.
		// If the last key changing operator changed the key type, it is recommended to
		// use groupByKey(org.apache.kafka.streams.kstream.Grouped) instead.
		KGroupedStream<String, Trade> kGroupedStream = kStream.groupByKey();

		// The fixed-size time-based window specifications used for aggregations.
		// The semantics of time-based aggregation windows are:
		// Every T1 (advance) milliseconds, compute the aggregate total for T2 (size)
		// milliseconds.
		// • If advance < size a hopping windows is defined:
		// it discretize a stream into overlapping windows, which implies that a record
		// may be contained in one and or more "adjacent" windows.
		// • If advance == size a tumbling window is defined:
		// it discretize a stream into non-overlapping windows, which implies that a
		// record is only ever contained in one and only one tumbling window.
		// Thus, the specified TimeWindows are aligned to the epoch.
		// Aligned to the epoch means, that the first window starts at timestamp zero.
		// For example, hopping windows with size of 5000ms and advance of 3000ms, have
		// window boundaries[0;5000),[3000;8000),... and not [1000;6000),[4000;9000),...
		// or even something "random" like [1452;6452),[4452;9452),...
		// For time semantics, see TimestampExtractor.

		// Return a window definition with the given window size, and with the advance
		// interval being equal to the windowsize.
		// The time interval represented by the N-th window is: [N * size, N * size +
		// size).
		// This provides the semantics of tumbling windows, which are fixed-sized,
		// gap-less, non-overlapping windows.
		// Tumbling windows are a special case of hopping windows with advance == size.
		// If the specified window size is zero or negative or can't be represented as
		// long milliseconds then exception is generated.

		// Return a window definition with the original size, but advance ("hop") the
		// window by the given interval, which specifies by how much a window moves
		// forward relative to the previous one.
		// The time interval represented by the N-th window is: [N * advance, N *
		// advance + size).
		// This provides the semantics of hopping windows, which are fixed-sized,
		// overlapping windows.
		// The advance interval ("hop") of the window, with the requirement that 0 <
		// advance.toMillis() <= sizeMs.
		// If the advance interval is negative, zero, or larger than the window size
		// then exception is generated.

		// The grace period to admit late-arriving events to a window.
		// Reject late events that arrive more than millisAfterWindowEnd after the end
		// of its window.
		// Lateness is defined as (stream_time - record_timestamp).
		TimeWindows timeWindows = TimeWindows.of(Duration.ofMillis(5000)).advanceBy(Duration.ofMillis(1000))
				.grace(Duration.ofMillis(1000));

		// TimeWindowedKStream is an abstraction of a windowed record stream of KeyValue
		// pairs.
		// It is an intermediate representation of a KStream in order to apply a
		// windowed aggregation operation on the original KStream records.
		// It is an intermediate representation after a grouping and windowing of a
		// KStream before an aggregation is applied to the new (partitioned) windows
		// resulting in a windowed KTable(a windowed KTable is a KTable with key type
		// Windowed).
		// The specified windows define either hopping time windows that can be
		// overlapping or tumbling (c.f. TimeWindows) or they define landmark windows
		// (c.f. UnlimitedWindows).
		// The result is written into a local windowed KeyValueStore (which is basically
		// an ever-updating materialized view) that can be queried using the name
		// provided in the Materialized instance.
		// New events are added to windows until their grace period ends (see
		// TimeWindows.grace(Duration)).
		// Furthermore, updates to the store are sent downstream into a windowed KTable
		// changelog stream, where"windowed" implies that the KTable key is a combined
		// key of the original record key and a window ID.
		// A WindowedKStream must be obtained from a KGroupedStream via
		// KGroupedStream.windowedBy(Windows).

		// Create a new TimeWindowedKStream instance that can be used to perform
		// windowed aggregations.
		TimeWindowedKStream<String, Trade> windowStream = kGroupedStream.windowedBy(timeWindows);

		// The Aggregator interface for aggregating values of the given key.
		// This is a generalization of Reducer and allows to have different types for
		// input value and aggregation result.
		// Aggregator is used in combination with Initializer that provides an initial
		// aggregation value.
		// Aggregator can be used to implement aggregation functions like count.
		Aggregator<String, Trade, TradeStatistics> aggregator = new Aggregator<String, Trade, TradeStatistics>() {

			@Override
			public TradeStatistics apply(String key, Trade value, TradeStatistics aggregate) {
				// TODO Auto-generated method stub
				return aggregate.add(value);
			}
		};

		// Used to describe how a StateStore should be materialized.
		// You can either provide a custom StateStore backend through one of the
		// provided methods accepting a supplieror use the default RocksDB backends by
		// providing just a store name.
		// For example, you can read a topic as KTable and force a state store
		// materialization to access the content via Interactive Queries API:

		// Interface for storing the aggregated values of fixed-size time windows.
		// Note, that the stores's key type is Windowed<K>.

		// Materialize a StateStore with the given name.

		// Set the valueSerde the materialized StateStore will use.
		Materialized<String, TradeStatistics, WindowStore<Bytes, byte[]>> materialized = Materialized
				.<String, TradeStatistics, WindowStore<Bytes, byte[]>>as("trade-stats-store")
				.withValueSerde(new TradeStatisticsSerde());

		// The result key type of a windowed stream aggregation.
		// If a KStream gets grouped and aggregated using a window-aggregation the
		// resulting KTable is a so-called "windowed KTable" with a combined key type
		// that encodes the corresponding aggregation window and the original record
		// key.
		// Thus, a windowed KTable has type <Windowed<K>,V>.

		// Aggregate the values of records in this stream by the grouped key.
		// Records with null key or value are ignored.
		// Aggregating is a generalization of combining via reduce(...) as it, for
		// example, allows the result to have a different type than the input values.
		// The result is written into a local KeyValueStore (which is basically an
		// ever-updating materialized view) that can be queried using the store name as
		// provided with Materialized.
		// The specified Initializer is applied once directly before the first input
		// record is processed to provide an initial intermediate aggregation result
		// that is used to process the first record.
		// The specified Aggregator is applied for each input record and computes a new
		// aggregate using the current aggregate (or for the very first record using the
		// intermediate aggregation result provided via the Initializer) and the
		// record's value.
		// Thus, aggregate(Initializer, Aggregator, Materialized) can be used to compute
		// aggregate functions like count (c.f. count()).
		// Not all updates might get sent downstream, as an internal cache will be used
		// to deduplicate consecutive updates to the same window and key if caching is
		// enabled on the Materialized instance.
		// When caching is enable the rate of propagated updates depends on your input
		// data rate, the number of distinct keys, the number of parallel running Kafka
		// Streams instances, and the configuration parameters for cache size, and
		// commit intervall
		// For failure and recovery the store will be backed by an internal changelog
		// topic that will be created in Kafka.
		// Therefore, the store name defined by the Materialized instance must be a
		// valid Kafka topic name and cannot contain characters other than ASCII
		// alphanumerics, '.', '_' and '-'.
		// The changelog topic will be named "${applicationId}-${storeName}-changelog",
		// where "applicationId" isuser-specified in StreamsConfig via parameter
		// APPLICATION_ID_CONFIG, "storeName" is the provide store name defined in
		// Materialized, and "-changelog" is a fixed suffix.
		// You can retrieve all generated internal topic names via Topology.describe().
		KTable<Windowed<String>, TradeStatistics> kTable = windowStream.aggregate(() -> new TradeStatistics(),
				aggregator, materialized);

		KStream<Windowed<String>, TradeStatistics> kStream1 = kTable.toStream();
		// kStream1.print(Printed.toSysOut());

		// Transform the value of each input record into a new value (with possible new
		// type) of the output record.
		// The provided ValueMapper is applied to each input record value and computes a
		// new value for it.
		// Thus, an input record <K,V> can be transformed into an output record <K:V'>.
		// This is a stateless record-by-record operation (cf.
		// transformValues(ValueTransformerSupplier, String...) for stateful value
		// transformation).
		// Setting a new value preserves data co-location with respect to the key.
		// Thus, no internal data redistribution is required if a key based operator
		// (like an aggregation or join) is applied to the result KStream. (cf.
		// map(KeyValueMapper))
		KStream<Windowed<String>, TradeStatistics> stream = kStream1
				.mapValues(new ValueMapper<TradeStatistics, TradeStatistics>() {

					@Override
					public TradeStatistics apply(TradeStatistics tradeStatistics) {
						// TODO Auto-generated method stub
						return tradeStatistics.computeAvgPrice();
					}

				});

		// Perform an action on each record of KStream.
		// This is a stateless record-by-record operation (cf.
		// process(ProcessorSupplier, String...)).
		// Note that this is a terminal operation that returns void.
		// The ForeachAction interface for performing an action on a key-value pair.
		// This is a stateless record-by-record operation, i.e, apply(Object, Object) is
		// invoked individually for each record of a stream.
		// If stateful processing is required, consider using KStream#process(...).
		stream.foreach(new ForeachAction<Windowed<String>, TradeStatistics>() {

			@Override
			public void apply(Windowed<String> key, TradeStatistics value) {
				// TODO Auto-generated method stub
				System.out.println("Key: " + key + " Value: " + value);
			}
		});

		// Get the name of the local state store used that can be used to query this
		// KTable.
		String queryableStoreName = kTable.queryableStoreName();
		System.out.println("Store Name: " + queryableStoreName);

		Produced<Windowed<String>, TradeStatistics> produced = Produced
				.with(WindowedSerdes.timeWindowedSerdeFrom(String.class), new TradeStatisticsSerde());

		stream.to(sinkTopicName, produced);
	}

	public static void main(String[] args) {

		StreamsBuilder streamBuilder = new StreamsBuilder();
		WordCountStreaming countStreaming = new WordCountStreaming();
		countStreaming.createWindowStreams(streamBuilder, "stocks", "stockstats-output");

		Topology topology = streamBuilder.build();
		final KafkaStreams kafkaStreams = new KafkaStreams(topology, countStreaming.properties);

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
}
