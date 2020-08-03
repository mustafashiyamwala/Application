package com.org.cassandra;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;

public class MultiThreading {

	private static Logger logger = Logger.getLogger(MultiThreading.class);

	public void multipleCall(int noOfCores, String query, Integer noOfRecords, String url, String keySpace,
			String tableName, String event) {

		int count = 0;
		Object result = null;
		Future<?> future = null;
		ExecutorService executorService = null;

		try {
			logger.info("No Of Thread: " + (noOfCores - 1));

			executorService = Executors.newFixedThreadPool(noOfCores - 1);
			RunnableTask runnableTask = new RunnableTask(query, url, keySpace, tableName, event);

			noOfRecords = event.equalsIgnoreCase("read") ? noOfCores - 1 : noOfRecords;

			for (int i = 0; i < noOfRecords; i++) {
				future = executorService.submit(runnableTask);

				try {
					if (!future.isCancelled() && future.isDone()) {
						result = future.get(200, TimeUnit.MILLISECONDS);
					}

				} catch (InterruptedException | ExecutionException | TimeoutException e) {
					// TODO: handle exception
					logger.error("Exception: " + e.getMessage());
				}

				count++;
				if (count % 1000 == 0) {
					logger.info("No of Records Inserted: " + count);
				}
			}

		} finally {
			executorService.shutdown();

			try {
				if (executorService.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
					executorService.shutdownNow();
				}

			} catch (Exception e) {
				// TODO: handle exception
				logger.error("Exception: " + e.getMessage());
				executorService.shutdown();
			}
		}
	}

	public void scheduleMultipleCall(String url, String keySpace, String tableName) {

		ScheduledExecutorService scheduledExecutorService = null;

		try {
			scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
			RunnableTask runnableTask = new RunnableTask("", url, keySpace, tableName, "count");

			ScheduledFuture<?> scheduledFuture = scheduledExecutorService.scheduleAtFixedRate(runnableTask, 100, 500,
					TimeUnit.MILLISECONDS);

			try {
				if (!scheduledFuture.isCancelled() && scheduledFuture.isDone()) {
					Object result = scheduledFuture.get(500, TimeUnit.MILLISECONDS);
				}

			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				// TODO: handle exception
				logger.error("Exception: " + e.getMessage());
			}

		} finally {

			try {
				if (scheduledExecutorService.awaitTermination(800, TimeUnit.MILLISECONDS)) {
					scheduledExecutorService.shutdownNow();
				}

			} catch (Exception e) {
				// TODO: handle exception
				logger.error("Exception: " + e.getMessage());
				scheduledExecutorService.shutdown();
			}
		}
	}
}
