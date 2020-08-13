package com.org.cassandra;

import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.utils.UUIDs;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.datastax.driver.mapping.Result;
import com.org.beans.BBData;
import com.org.beans.BBVars;
import com.org.exception.SparkCassandraException;
import com.org.service.BBService;
import com.org.utils.CSVUtility;

public class CassandraReadWrite {

	private String[] urls;
	private Integer port;
	private String userName;
	private String password;
	private String keySpace;
	private String tableName;
	private Session session;
	private Cluster cluster;
	private Path filePath;
	private QueryOptions queryOptions;
	private CodecRegistry codecRegistry;
	private static Logger logger = Logger.getLogger(CassandraReadWrite.class);

	public CassandraReadWrite(String[] urls, Integer port, String userName, String password, String keySpace,
			String tableName) {
		this.urls = urls;
		this.port = port;
		this.userName = userName;
		this.password = password;
		this.keySpace = keySpace;
		this.tableName = tableName;
		this.queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE);
		this.codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
	}

	public CassandraReadWrite(String[] urls, Integer port, String userName, String password, String keySpace,
			String tableName, Path filePath) {
		this.urls = urls;
		this.port = port;
		this.userName = userName;
		this.password = password;
		this.keySpace = keySpace;
		this.tableName = tableName;
		this.filePath = filePath;
		this.queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE);
		this.codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
	}

	public void connect() throws SparkCassandraException {
		try {

			Cluster.Builder builder = Cluster.builder();
			Cluster.Builder builder1 = builder.addContactPoints(this.urls).withPort(this.port)
					.withAuthProvider(new PlainTextAuthProvider(this.userName, this.password))
					.withQueryOptions(this.queryOptions).withCodecRegistry(this.codecRegistry);
			this.cluster = builder1.build();
			this.session = cluster.connect(this.keySpace);

			logger.info("Cassandra Connection is Done");

		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Exception: " + e.getMessage());
			throw new SparkCassandraException("Exception:" + e.getMessage());
		}
	}

	public void write() {

		try {
			int count = 0;
			// BatchStatement batchStatement = new BatchStatement();
			// String insertQuery = "INSERT INTO " + this.keySpace + "." + this.tableName
			// + " (bbluuid, beanclassname, datehourbucket, fullyprocessed, startblock, ts,
			// vars) VALUES(?, ?, ?, ?, ?, ?, ?)";

			connect();
			// PreparedStatement preparedStatement = this.session.prepare(insertQuery);
			Mapper<BBData> mapper = new MappingManager(this.session).mapper(BBData.class);

			Iterable<CSVRecord> iterable = new CSVUtility().readCSVFile(filePath.toString());
			long nanosec = ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId());

			for (CSVRecord csvRecord : iterable) {

				/*
				 * UserType userType =
				 * this.session.getCluster().getMetadata().getKeyspace(this.keySpace)
				 * .getUserType("BBVars"); UDTValue udtValue =
				 * userType.newValue().setDouble("mt_aiflo",
				 * Double.parseDouble(csvRecord.get(0))) .setDouble("mt_aitmp",
				 * Double.parseDouble(csvRecord.get(1))) .setDouble("mt_aiden",
				 * Double.parseDouble(csvRecord.get(2)));
				 * 
				 * batchStatement.add(preparedStatement.bind().setUUID("bbluuid",
				 * UUIDs.timeBased()) .setString("beanclassname", BBData.class.getSimpleName())
				 * .setString("datehourbucket",
				 * LocalDateTime.now().toString()).setBool("fullyprocessed", false)
				 * .setBool("startblock", false).setString("ts",
				 * String.valueOf(Instant.now().toEpochMilli())) .setUDTValue("vars",
				 * udtValue));
				 */

				BBVars bbVars = new BBVars(Double.parseDouble(csvRecord.get(0)), Double.parseDouble(csvRecord.get(1)),
						Double.parseDouble(csvRecord.get(2)));
				BBData bbData = new BBData(UUIDs.random(), BBData.class.getSimpleName(), LocalDateTime.now().toString(),
						false, false, String.valueOf(Instant.now().toEpochMilli()), bbVars);

				Statement statement = mapper.saveQuery(bbData);
				this.session.execute(statement);
				count++;
				
				/*
				 * batchStatement.add(statement); 
				 * 
				 * if (count % 100 == 0) { this.session.execute(batchStatement); batchStatement
				 * = new BatchStatement(); logger.info("Records Inserted: =============> " +
				 * count); }
				 */
			}
			// this.session.execute(batchStatement);
			logger.info("No of Records Inserted: ======> " + count);
			close();

			long nanosec1 = ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId());
			logger.info("Time:  " + (nanosec1 - nanosec) / 1000000 + " Thread: " + Thread.currentThread().getId());

		} catch (SparkCassandraException e) {
			// TODO Auto-generated catch block
			logger.error("Exception: " + e.getMessage());
		}
	}

	public void read() {
		try {
			long nanosec = ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId());

			connect();
			String query = "SELECT * FROM " + this.keySpace + "." + this.tableName;

			Mapper<BBData> mapper = new MappingManager(this.session).mapper(BBData.class);
			Result<BBData> result = mapper.map(this.session.execute(query));
			List<BBData> list = result.all();

			// ResultSet resultSet = this.session.execute(query);
			// List<Row> rows = resultSet.all();

			logger.info("No Of Records Retrieve:  ======> " + list.size());
			close();

			long nanosec1 = ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId());
			logger.info("Time:  " + (nanosec1 - nanosec) / 1000000 + " Thread: " + Thread.currentThread().getId());

		} catch (SparkCassandraException e) {
			// TODO Auto-generated catch block
			logger.error("Exception: " + e.getMessage());
		}
	}

	public void count() {
		try {
			connect();
			ResultSet resultSet = session.execute("SELECT COUNT(*) FROM " + this.keySpace + "." + this.tableName);
			Row row = resultSet.one();
			logger.info("Count ==========> " + row);
			close();

		} catch (SparkCassandraException e) {
			// TODO Auto-generated catch block
			logger.error("Exception: " + e.getMessage());
		}
	}

	public void close() {
		try {
			this.session.close();
			this.cluster.close();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error("Exception: " + e.getMessage());
		}
	}

	/*
	 * public void setUp() { BundleContext bundleContext =
	 * FrameworkUtil.getBundle(BBService.class).getBundleContext(); ServiceReference
	 * reference = bundleContext.getServiceReference(BBService.class.getName());
	 * BBService service = (BBService) bundleContext.getService(reference);
	 * //service.sendMessage(message); }
	 */
}
