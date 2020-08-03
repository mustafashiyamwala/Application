package com.org.cassandra;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class MutliRead {

	private static Logger logger = Logger.getLogger(MutliRead.class);

	public static void main(String[] args) {

		try (InputStream inputStream = new FileInputStream("src/main/resources/input.properties")) {

			Properties properties = new Properties();
			properties.load(inputStream);
			logger.info("Load External Properties File");

			String url = properties.getProperty("url");
			String keySpace = properties.getProperty("keySpace");
			String tableName = properties.getProperty("tableName");
			
			int noOfCores = Runtime.getRuntime().availableProcessors();
			logger.info("Load all Properties");

			MultiThreading multiThreading = new MultiThreading();
			multiThreading.multipleCall(noOfCores, "", 0, url, keySpace, tableName, "read");

			logger.info("Read Operation Completed");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("Exception: " + e.getMessage());
		}
	}
}
