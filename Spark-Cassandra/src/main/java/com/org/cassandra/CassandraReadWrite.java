package com.org.cassandra;

import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;
import org.osgi.framework.BundleContext;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
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
	private static Logger logger = Logger.getLogger(CassandraReadWrite.class);

	public CassandraReadWrite(String[] urls, Integer port, String userName, String password, String keySpace,
			String tableName) {
		this.urls = urls;
		this.port = port;
		this.userName = userName;
		this.password = password;
		this.keySpace = keySpace;
		this.tableName = tableName;
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
	}

	public void connect() throws SparkCassandraException {
		try {
			Cluster.Builder builder = Cluster.builder();
			Cluster.Builder builder1 = builder.addContactPoints(this.urls).withPort(this.port)
					.withAuthProvider(new PlainTextAuthProvider(this.userName, this.password));
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
			BatchStatement batchStatement = new BatchStatement();
			/*
			 * String insertQuery = "INSERT INTO " + this.keySpace + "." + this.tableName +
			 * " (bbluuid, beanclassname, datehourbucket, fullyprocessed, startblock, ts,
			 * vars) VALUES(?, ?, ?, ?, ?, ?, ?)";
			 */

			connect();
			Iterable<CSVRecord> iterable = new CSVUtility().readCSVFile(filePath.toString());

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
				 * PreparedStatement preparedStatement = this.session.prepare(insertQuery);
				 * batchStatement.add(preparedStatement.bind().setUUID("bbluuid",
				 * UUIDs.timeBased()) .setString("beanclassname", BBData.class.getSimpleName())
				 * .setString("datehourbucket",
				 * LocalDateTime.now().toString()).setBool("fullyprocessed", false)
				 * .setBool("startblock", false).setString("ts",
				 * String.valueOf(Instant.now().toEpochMilli())) .setUDTValue("vars",
				 * udtValue));
				 */

				Mapper<BBData> mapper = new MappingManager(this.session).mapper(BBData.class);

				BBVars bbVars = new BBVars(Double.parseDouble(csvRecord.get(0)), Double.parseDouble(csvRecord.get(1)),
						Double.parseDouble(csvRecord.get(2)));
				BBData bbData = new BBData(UUIDs.timeBased(), BBData.class.getSimpleName(),
						LocalDateTime.now().toString(), false, false, String.valueOf(Instant.now().toEpochMilli()),
						bbVars);

				Statement statement = mapper.saveQuery(bbData);
				batchStatement.add(statement);
				count++;

				if (count % 1000 == 0) {
					this.session.executeAsync(batchStatement);
					batchStatement = new BatchStatement();
				}
			}
			this.session.execute(batchStatement);

			close();

		} catch (SparkCassandraException e) {
			// TODO Auto-generated catch block
			logger.error("Exception: " + e.getMessage());
		}
	}

	public void read() {
		try {
			connect();
			String query = "SELECT * FROM " + this.keySpace + "." + this.tableName;

			Mapper<BBData> mapper = new MappingManager(this.session).mapper(BBData.class);
			Result<BBData> result = mapper.map(this.session.execute(query));
			List<BBData> list = result.all();

			// ResultSet resultSet = this.session.execute(query);
			// List<Row> rows = resultSet.all();

			logger.info("No Of Records Retrieve: " + list.size());
			close();

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
			logger.info("Count ===============> " + row);
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
