package com.org.stream.join;

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
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.KeyValueStore;

public class ClickEnrichment {

	private Properties properties;

	public ClickEnrichment() {
		configuringStream();
	}

	private void configuringStream() {
		this.properties = new Properties();
		this.properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "click");
		this.properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.BROKER);
		this.properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		this.properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		this.properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

		this.properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
		this.properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "1000");
		this.properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "1024");
		// this.properties.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, "all");
		this.properties.put(StreamsConfig.STATE_DIR_CONFIG, "src/main/resources");
	}

	private void createJoinStream(StreamsBuilder streamsBuilder) throws InterruptedException {

		Consumed<Integer, PageView> consumedPageView = Consumed.with(Serdes.Integer(), new PageViewSerde());

		KStream<Integer, PageView> viewStream = streamsBuilder.stream(Constants.PAGE_VIEW_TOPIC, consumedPageView);

		viewStream.print(Printed.toSysOut());

		Consumed<Integer, Search> consumedSearch = Consumed.with(Serdes.Integer(), new SearchSerde());

		KStream<Integer, Search> searchStream = streamsBuilder.stream(Constants.SEARCH_TOPIC, consumedSearch);

		searchStream.print(Printed.toSysOut());

		Consumed<Integer, UserProfile> consumedUserProfile = Consumed.with(Serdes.Integer(), new UserProfileSerde());

		// Create a KTable for the specified topic.
		// The "auto.offset.reset" strategy, TimestampExtractor, key and value
		// deserializers are defined by the options in Consumed are used.
		// Input records with null key will be dropped.
		// Note that the specified input topic must be partitioned by key.
		// If this is not the case the returned KTable will be corrupted.
		// The resulting KTable will be materialized in a local KeyValueStore using the
		// given Materialized instance.
		// An internal changelog topic is created by default.
		// Because the source topic can be used for recovery, you can avoid creating the
		// changelog topic by setting the "topology.optimization" to "all" in the
		// StreamsConfig.
		// You should only specify serdes in the Consumed instance as these will also be
		// used to overwrite the serdes in Materialized (Hence there is no need of
		// defining serdes in Materialized instance)
		// For non-local keys, a custom RPC mechanism must be implemented using
		// KafkaStreams.allMetadata() to query the value of the key on a parallel
		// running instance of your Kafka Streams application.

		// A key-value store that supports put/get/delete and range queries.
		KTable<Integer, UserProfile> profileTable = streamsBuilder.table(Constants.USER_PROFILE_TOPIC,
				consumedUserProfile,
				Materialized.<Integer, UserProfile, KeyValueStore<Bytes, byte[]>>as("profile-store"));

		profileTable.toStream().print(Printed.toSysOut());

		// Join records of this stream with KTable's, KStream's records using
		// non-windowed or windowed left equi join with default serializers and
		// deserializers.
		// In contrast to inner-join, all records from this stream will produce an
		// output record (cf. below).
		// The join is a primary key table lookup join with join attribute stream.key ==
		// table.key.
		// "Table lookup join" means, that results are only computed if KStream records
		// are processed.
		// This is done by performing a lookup for matching records in the current
		// (i.e., processing time) internal KTable state or KStream.
		// In contrast, processing KTable input records will only update the internal
		// KTable state and will not produce any result records.
		// For each KStream record whether or not it finds a corresponding record in
		// KTable or KStream the provided ValueJoiner will be called to compute a value
		// (with arbitrary type) for the result record.
		// If no KTable record was found during lookup, a null value will be provided to
		// ValueJoiner.
		// The key of the result record is the same as for both joining input records.
		// If an KStream input record key or value is null the record will not be
		// included in the join operation and thus no output record will be added to the
		// resulting KStream.
		// Both input streams (or to be more precise, their underlying source topics)
		// need to have the same number of partitions.
		// If this is not the case, you would need to call through(String) for this
		// KStream before doing the join, using a pre-created topic with the same number
		// of partitions as the given KTable.
		// Furthermore, both input streams need to be co-partitioned on the join key
		// (i.e., use the same partitioner);cf. join(GlobalKTable, KeyValueMapper,
		// ValueJoiner).
		// If this requirement is not met, Kafka Streams will automatically repartition
		// the data, i.e., it will create an internal repartitioning topic in Kafka and
		// write and re-read the data via this topic before the actual join.
		// The repartitioning topic will be named "${applicationId}-<name>-repartition",
		// where "applicationId" isuser-specified in StreamsConfig via parameter
		// APPLICATION_ID_CONFIG, "<name>" is an internally generated name, and
		// "-repartition" is a fixed suffix.
		// You can retrieve all generated internal topic names via Topology.describe().
		// Repartitioning can happen only for this KStream but not for the provided
		// KTable.
		// For this case, all data of the stream will be redistributed through the
		// repartitioning topic by writing all records to it, and rereading all records
		// from it, such that the join input KStream is partitioned correctly on its
		// key.

		// The ValueJoiner interface for joining two values into a new value of
		// arbitrary type.
		// This is a stateless operation, i.e, apply(Object, Object) is invoked
		// individually for each joining record-pair of a KStream-KStream,
		// KStream-KTable, or KTable-KTable join.
		KStream<Integer, UserActivity> userActivityStream = viewStream.leftJoin(profileTable,
				new ValueJoiner<PageView, UserProfile, UserActivity>() {

					@Override
					public UserActivity apply(PageView stream, UserProfile table) {
						// TODO Auto-generated method stub

						if (table != null) {
							return new UserActivity(table.getUserID(), table.getUserName(), table.getZipcode(),
									table.getInterests(), "", stream.getPage());
						}
						return new UserActivity(-1, "", "", new String[] {}, "", stream.getPage());
					}
				});

		userActivityStream.print(Printed.toSysOut());

		// The Joined class represents optional params that can be passed to
		// KStream.join(org.apache.kafka.streams.kstream.KStream<K, VO>,
		// org.apache.kafka.streams.kstream.ValueJoiner<? super V, ? super VO, ? extends
		// VR>, org.apache.kafka.streams.kstream.JoinWindows) operations.

		// Create an instance of Joined with key, value, and otherValue Serde instances.
		// null values are accepted and will be replaced by the default serdes as
		// defined in config.
		Joined<Integer, UserActivity, Search> joined = Joined.with(Serdes.Integer(), new UserActivitySerde(),
				new SearchSerde());

		KStream<Integer, UserActivity> userActivityKStream = userActivityStream.leftJoin(searchStream,

				new ValueJoiner<UserActivity, Search, UserActivity>() {
					@Override
					public UserActivity apply(UserActivity stream1, Search stream2) {
						// TODO Auto-generated method stub

						if (stream2 != null) {
							return stream1.updateSearch(stream2.getSearchTerms());
						}
						return stream1.updateSearch("");
					}
				}, JoinWindows.of(Duration.ofMillis(1000)), joined);

		// Perform an action on each record of KStream.
		// This is a stateless record-by-record operation (cf.
		// process(ProcessorSupplier, String...)).
		// Peek is a non-terminal operation that triggers a side effect (such as logging
		// or statistics collection)and returns an unchanged stream.
		// Note that since this operation is stateless, it may execute multiple times
		// for a single record in failure cases.
		userActivityKStream.peek(new ForeachAction<Integer, UserActivity>() {

			@Override
			public void apply(Integer key, UserActivity value) {
				// TODO Auto-generated method stub
				System.out.println("Key: " + key + " Value: " + value);
			}
		});

		userActivityKStream.print(Printed.toSysOut());

		Produced<Integer, UserActivity> produced = Produced.with(Serdes.Integer(), new UserActivitySerde());

		userActivityKStream.to(Constants.USER_ACTIVITY_TOPIC, produced);

	}

	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub

		ClickEnrichment clickEnrichment = new ClickEnrichment();
		StreamsBuilder streamsBuilder = new StreamsBuilder();

		clickEnrichment.createJoinStream(streamsBuilder);

		Topology topology = streamsBuilder.build();
		KafkaStreams kafkaStreams = new KafkaStreams(topology, clickEnrichment.properties);

		final CountDownLatch latch = new CountDownLatch(1);

		// attach shutdown handler to catch control-c
		Runtime.getRuntime().addShutdownHook(new Thread("click-enrichment") {
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
