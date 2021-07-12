package com.org.cassandra;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import com.org.exception.CassandraException;

public class MultiWrite {

	private static Logger logger = Logger.getLogger(MultiWrite.class);

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		DOMConfigurator.configure(MultiWrite.class.getResource("/log4j.xml"));

		try (InputStream inputStream = new FileInputStream(args[0])) {

			Properties properties = new Properties();
			properties.load(inputStream);
			logger.info("Load External Properties File");

			String path = properties.getProperty("filePath").trim();
			String[] urls = properties.getProperty("urls") != null ? properties.getProperty("urls").split(",")
					: new String[] { "localhosts" };
			Integer port = Integer.parseInt(
					properties.getProperty("port").trim() != null ? properties.getProperty("port").trim() : "9042");
			String userName = properties.getProperty("userName").trim();
			String password = properties.getProperty("password").trim();
			String keySpace = properties.getProperty("keySpace").trim();
			String tableName = properties.getProperty("tableName").trim();

			int noOfCores = Runtime.getRuntime().availableProcessors();
			logger.info("Number of Cores Available: " + noOfCores);

			Integer noOfThreads = Integer.parseInt(properties.getProperty("noOfThreads").trim());
			// Integer maxParallelExexcution = noOfThreads > noOfCores ? noOfCores :
			// noOfThreads;
			Integer maxParallelExexcution = noOfThreads;
			logger.info("Max Parallel Execution: " + maxParallelExexcution);

			String events = properties.getProperty("events").trim();
			logger.info("Successfully Load all Properties");

			DirectoryStream<Path> directoryStream = Files.exists(Paths.get(path))
					? Files.newDirectoryStream(Paths.get(path), fileName -> fileName.toString().endsWith(".csv"))
					: null;

			List<Path> filePath = StreamSupport.stream(directoryStream.spliterator(), false)
					.collect(Collectors.toList());
			logger.info("No of CSV Files to Ingest Records in Cassandra Tables: " + filePath.size());

			MultiThreading multiThreading = new MultiThreading();

			if (events.equalsIgnoreCase("write") || events.equalsIgnoreCase("read")) {
				multiThreading.multipleCall(urls, port, userName, password, keySpace, tableName, maxParallelExexcution,
						filePath, events);

			} else {
				// multiThreading.scheduleMultipleCall(urls, keySpace, tableName);
			}

			logger.info("Write Operation Completed");

		} catch (IOException | CassandraException e) {
			// TODO Auto-generated catch block
			logger.error("Exception: " + e.getMessage());
		}
	}
}
