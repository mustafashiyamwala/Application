package com.org.cassandra;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.log4j.Logger;
import com.org.exception.CassandraException;

public class MultiThreading {

	private static Logger logger = Logger.getLogger(MultiThreading.class);

	public void multipleCall(String[] urls, Integer port, String userName, String password, String keySpace,
			String tableName, Integer maxParallelExexcution, List<Path> directoryStream, String event)
			throws CassandraException {

 		Object result = null;
		List<Future<?>> resultFuture = new ArrayList<Future<?>>();
		List<RunnableTask> runnableTask = new ArrayList<RunnableTask>();
		ExecutorService executorService = null;

		if (event.equalsIgnoreCase("write")) {
			for (Path filePath : directoryStream) {
				runnableTask
						.add(new RunnableTask(urls, port, userName, password, keySpace, tableName, filePath, event));
			}

		} else if (event.equalsIgnoreCase("read")) {
			for (int i = 0; i < maxParallelExexcution; i++) {
				runnableTask.add(new RunnableTask(urls, port, userName, password, keySpace, tableName, event));
			}
		}

		try {
			executorService = Executors.newFixedThreadPool(maxParallelExexcution);

			try {
				logger.info("Execution Started");

				for (RunnableTask task : runnableTask) {
					resultFuture.add(executorService.submit(task));
				}

				for (Future<?> future : resultFuture) {
					
					if (!future.isCancelled() && future.isDone()) {
						result = future.get(200, TimeUnit.MILLISECONDS);
					}
				}
				logger.info("Execution Completed");

			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				// TODO: handle exception
				logger.error("Exception: " + e.getMessage());
				throw new CassandraException("Exception: " + e.getMessage());
			}

		} finally {
			executorService.shutdown();

			try {
				if (executorService.awaitTermination(2000, TimeUnit.MILLISECONDS)) {
					executorService.shutdownNow();
				}

			} catch (Exception e) {
				// TODO: handle exception
				logger.error("Exception: " + e.getMessage());
				executorService.shutdown();
				throw new CassandraException("Exception: " + e.getMessage());
			}
		}
	}

	/*
	 * public void scheduleMultipleCall(String url, String keySpace, String
	 * tableName) {
	 * 
	 * ScheduledExecutorService scheduledExecutorService = null;
	 * 
	 * try { scheduledExecutorService =
	 * Executors.newSingleThreadScheduledExecutor(); RunnableTask runnableTask = new
	 * RunnableTask(url, keySpace, tableName, "count");
	 * 
	 * ScheduledFuture<?> scheduledFuture =
	 * scheduledExecutorService.scheduleAtFixedRate(runnableTask, 100, 500,
	 * TimeUnit.MILLISECONDS);
	 * 
	 * try { if (!scheduledFuture.isCancelled() && scheduledFuture.isDone()) {
	 * Object result = scheduledFuture.get(500, TimeUnit.MILLISECONDS); }
	 * 
	 * } catch (InterruptedException | ExecutionException | TimeoutException e) { //
	 * TODO: handle exception logger.error("Exception: " + e.getMessage()); }
	 * 
	 * } finally {
	 * 
	 * try { if (scheduledExecutorService.awaitTermination(800,
	 * TimeUnit.MILLISECONDS)) { scheduledExecutorService.shutdownNow(); }
	 * 
	 * } catch (Exception e) { // TODO: handle exception logger.error("Exception: "
	 * + e.getMessage()); scheduledExecutorService.shutdown(); } } }
	 */
}
