package com.org.cassandra;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class Consistency {

	private static Logger logger = Logger.getLogger(Consistency.class);

	public static void main(String[] args) {

		try (InputStream inputStream = new FileInputStream("src/main/resources/input.properties")) {

			Properties properties = new Properties();
			properties.load(inputStream);
			logger.info("Load External Properties File");

			String url = properties.getProperty("url");
			String keySpace = properties.getProperty("keySpace");
			String tableName = properties.getProperty("tableName");

			logger.info("Load all Properties");

			MultiThreading multiThreading = new MultiThreading();
			multiThreading.scheduleMultipleCall(url, keySpace, tableName);

			logger.info("Count Operation Completed");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Exception: " + e.getMessage());
		}
	}
}
