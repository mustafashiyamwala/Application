package com.org.cassandra;

import java.util.List;

import org.apache.log4j.Logger;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraReadWrite {

	private String url;
	private String keySpace;
	private String query;
	private String tableName;
	private Session session;
	private Cluster cluster;
	private static Logger logger = Logger.getLogger(CassandraReadWrite.class);

	public CassandraReadWrite(String query, String url, String keySpace, String tableName) {
		this.url = url;
		this.query = query;
		this.keySpace = keySpace;
		this.tableName = tableName;

	}

	public void connect() {
		Cluster.Builder builder = Cluster.builder();//.withClusterName("Spark-Cassandra");
		Cluster.Builder builder1 = builder.addContactPoint(url);
		this.cluster = builder1.build();
		this.session = cluster.connect(keySpace);

		logger.info("Cassandra Connection is Done");
	}

	public void write() {
		
		connect();
		this.session.execute(this.query);
		close();
	}

	public void read() {
		
		connect();
		int count = 0;

		ResultSet resultSet = this.session.execute("SELECT * FROM " + this.tableName);
		List<Row> rows = resultSet.all();

		for (Row row : rows) {

			count++;
			if (count % 1000 == 0) {
				logger.info("Records: " + row);
				logger.info("No Of Records Inserted: " + count);
			}
			
		}
		logger.info("No Of Records Inserted: " + count);
		close();
	}

	public void count() {

		connect();
		ResultSet resultSet = session.execute("SELECT COUNT(*) FROM " + this.tableName);
		Row row = resultSet.one();
		logger.info("Count ===============> " + row);
		close();
	}

	public void close() {

		this.session.close();
		this.cluster.close();
	}
}
