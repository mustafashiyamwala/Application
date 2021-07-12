package com.org.cassandra;

import java.nio.file.Path;

import org.apache.log4j.Logger;

public class RunnableTask implements Runnable {

	private String event;
	private CassandraReadWrite cassandraReadWrite;
	private static Logger logger = Logger.getLogger(RunnableTask.class);

	public RunnableTask(String[] urls, Integer port, String userName, String password, String keySpace,
			String tableName, String event) {
		// TODO Auto-generated constructor stub
		this.event = event;
		this.cassandraReadWrite = new CassandraReadWrite(urls, port, userName, password, keySpace, tableName);
		logger.info(event + " Task is Initialized ");
	}

	public RunnableTask(String[] urls, Integer port, String userName, String password, String keySpace,
			String tableName, Path filePath, String event) {
		// TODO Auto-generated constructor stub
		this.event = event;
		this.cassandraReadWrite = new CassandraReadWrite(urls, port, userName, password, keySpace, tableName, filePath);
		logger.info(event + " Task is Initialized ");
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		if (this.event.equalsIgnoreCase("write")) {
			this.cassandraReadWrite.write();

		} else if (this.event.equalsIgnoreCase("count")) {
			this.cassandraReadWrite.count();
		}

		else if (this.event.equalsIgnoreCase("read")) {
			this.cassandraReadWrite.read();
		}
	}
}
