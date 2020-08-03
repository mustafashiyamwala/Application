package com.org.cassandra;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Logger;

public class MultiWrite {

	private static Logger logger = Logger.getLogger(MultiWrite.class);

	@SuppressWarnings("resource")
	public static void main(String[] args) {

		System.getProperties().setProperty("com.datastax.driver.USE_NATIVE_CLOCK", "false");
		
		try (InputStream inputStream = new FileInputStream("src/main/resources/input.properties")) {

			Properties properties = new Properties();
			properties.load(inputStream);
			logger.info("Load External Properties File");

			String path = properties.getProperty("queryPath");
			String url = properties.getProperty("url");
			String keySpace = properties.getProperty("keySpace");
			String tableName = properties.getProperty("tableName");
			Integer noOfRecords = Integer.parseInt(properties.getProperty("noOfRecords").trim());

			int noOfCores = Runtime.getRuntime().availableProcessors();
			logger.info("Load all Properties");

			Stream<String> stream = Files.lines(Paths.get(path));
			String query = stream.collect(Collectors.joining());
			logger.info("Load Insert Query");

			MultiThreading multiThreading = new MultiThreading();
			multiThreading.multipleCall(noOfCores, query, noOfRecords, url, keySpace, tableName, "write");

			logger.info("Write Operation Completed");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("Exception: " + e.getMessage());
		}
	}
}
