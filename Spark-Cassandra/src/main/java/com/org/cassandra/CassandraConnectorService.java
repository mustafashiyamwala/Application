package com.org.cassandra;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Cluster.Builder;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.Statement;
import com.datastax.driver.mapping.MappingManager;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.org.bundle.StatementBean;
import com.org.exception.CassandraException;

/**
 * Get the Cluster Session
 * <p>
 * 
 * It is recommended to use one session to reuse resources so it is setup as a
 * single service.<br>
 * The datastax session is thread-safe
 * 
 * TODO: We can develop a process to use multiple sessions, each with its own
 * keyspace, if we are in need of better performance
 * 
 */
public class CassandraConnectorService {
	private final String className = CassandraConnectorService.class.getName();

	private String[] address = { "127.0.0.1" };
	private int port = 0;// Port 9042 is default
	private int timeout = 2000;
	private int fetchSize = 500;
	private String keyspace = null;
	private final String propertyFileDirectory = File.separator + "OSGiSystem" + File.separator + "Database";
	private final String propertyFileName = "cassandra.properties";

	private Cluster cluster = null;
	private Session session = null;
	private MappingManager mappingManager = null;

	/*
	 * Build the locking mechanisms for better performance on the services while
	 * still having locking capabilities needed for OSGi updates
	 */
	private final ReentrantReadWriteLock disconnectionBufferLock = new ReentrantReadWriteLock(true);
	private final ReentrantReadWriteLock connectorLock = new ReentrantReadWriteLock(true);
	private final Object mutexPrepare = new Object();
	private final Object bufferListMutex = new Object();

	/* Prepared statement map to contain all prepared statements of the connector */
	private final Map<String, PreparedStatement> preparedStmtMap = new HashMap<String, PreparedStatement>();

	/* Statement Objects */
	private List<StatementBean> bufferingStatements = new ArrayList<StatementBean>();
	private final BlockingQueue<List<StatementBean>> queue = new LinkedBlockingQueue<List<StatementBean>>();
	/* 256 max threads for the DataStax java connector */
	/* Leave more than enough for other processes to use the remaining threads */
	private final int maxBlockProcessor = 50;

	private boolean queueProcessing = false;

	private boolean shutdown = false;
	private Thread insertThread;

	public CassandraConnectorService() {
		super();
	}

	public void activate() {
		// logger.info(className, "Activate");

		try {
			updateProperties();

			connect(address, port, keyspace);

			bufferingStatements = new ArrayList<StatementBean>();
			queue.clear();
			shutdown = false;

			insertThread = new Thread(this::queueRunner);
			insertThread.start();

		} catch (CassandraException e) {
			e.printStackTrace();
		}
	}

	public void deactivate() {
		// logger.info(className, "Deactivate...");

		close();

		// logger.info(className, "...Deactivated");
	}

	/**
	 * Get the update from the properties file
	 * 
	 */
	private void updateProperties() {
		final File propertiesDirectory = new File(propertyFileDirectory);
		if (!propertiesDirectory.exists())
			propertiesDirectory.mkdirs();
		else {
			try (InputStream inputStream = new FileInputStream("src/main/resources/input.properties")) {

				Properties properties = new Properties();
				properties.load(inputStream);
				// logger.info("Load External Properties File");

				String path = properties.getProperty("filePath").trim();
				address = properties.getProperty("urls") != null ? properties.getProperty("urls").split(",")
						: new String[] { "localhosts" };
				port = Integer.parseInt(
						properties.getProperty("port").trim() != null ? properties.getProperty("port").trim() : "9042");
				String userName = properties.getProperty("userName").trim();
				String password = properties.getProperty("password").trim();
				keyspace = properties.getProperty("keySpace").trim();
				String tableName = properties.getProperty("tableName").trim();
				timeout = Integer.parseInt(properties.getProperty("timeout").trim());
				fetchSize = Integer.parseInt(properties.getProperty("fetchSize").trim());

				/*
				 * Property p; final File propertiesFile = new File(propertyFileDirectory +
				 * File.separator + propertyFileName); if (propertiesFile.exists()) p = new
				 * Property(propertiesFile.getAbsolutePath()); else return;
				 * 
				 * if (p.retrieveAllFileProperties()) { String prop;
				 * 
				 * /* IP Address(es)
				 */
				/*
				 * prop = p.getProperty("address");// comma separated address if (prop != null)
				 * { final Validate v = new Validate(); final List<String> finalAddresses = new
				 * ArrayList<String>(); String[] addresses = prop.split(",");
				 * 
				 * /* Validate the IP Addresses
				 */
				/*
				 * for (String s : addresses) if (v.isIpAddress(s)) finalAddresses.add(s);
				 * 
				 * address = new String[finalAddresses.size()]; address =
				 * finalAddresses.toArray(address); }
				 * 
				 * /* Port Number
				 */
				/*
				 * prop = p.getProperty("port"); if (prop != null) port = new
				 * DataHelper().safeIntInsert(prop); else port = 0;
				 * 
				 * /* Timeout
				 */
				/*
				 * prop = p.getProperty("timeout"); if (prop != null) timeout = new
				 * DataHelper().safeIntInsert(prop);
				 * 
				 * /* Timeout
				 */
				/*
				 * prop = p.getProperty("fetchSize"); if (prop != null) fetchSize = new
				 * DataHelper().safeIntInsert(prop);
				 * 
				 * /* Key space
				 */
				/*
				 * prop = p.getProperty("keyspace"); if (prop != null) keyspace = prop;
				 * 
				 * logger.info(className, p.toString()); }
				 */
			} catch (IOException e) {
				// TODO Auto-generated catch block
				// logger.error("Exception: " + e.getMessage());
			}
		}
	}

	/**
	 * Get the connection session of the cluster with the key space<br>
	 * This method is synchronized at first call using the double checked lock
	 * <p>
	 * 
	 * <b>NOTE:</b> It is better to use the key-space-less session to avoid
	 * locks<br>
	 * 
	 * @param address
	 * @param port     0 if using the default port
	 * @param keyspace <code>null</code> (Recommended) if no keyspace is being used
	 *                 for the session
	 * @throws CassandraException
	 */
	private void connect(String[] address, int port, String keyspace) throws CassandraException {
		connectorLock.writeLock().lock();
		try {
			/*
			 * Double check after the lock to make sure another thread did not setup the
			 * connection after coming out of the lock
			 */
			if (cluster == null || session == null)
				buildConnection(address, port, keyspace);

		} finally {
			connectorLock.writeLock().unlock();
		}
	}

	/**
	 * Connect the session
	 * 
	 * @param address
	 * @param port     0 if using the default port
	 * @param keyspace <code>null</code> (Recommended) if no keyspace is being used
	 *                 for the session
	 * @throws Exception
	 */
	private void buildConnection(String[] address, int port, String keyspace) throws CassandraException {
		// logger.info(className, "buildConnection({}, {}, {})",
		// Arrays.toString(address), port, keyspace);

		try {
			/* Attempt to close any resources that could have been left open */
			if (session != null) {
				session.close();
				session = null;
			}
			if (cluster != null) {
				cluster.close();
				cluster = null;
			}

			final Builder b = Cluster.builder().addContactPoints(address)
					.withSocketOptions(new SocketOptions().setConnectTimeoutMillis(timeout));

			if (port != 0)
				b.withPort(port);

			cluster = b.build();

			if (keyspace != null)
				session = cluster.connect(keyspace);
			else
				session = cluster.connect();

			/* Build the object Mapping Manager for the session */
			mappingManager = new MappingManager(session);

			// logger.info(className, "Cassandra Connection Service: {}, Driver Version: {},
			// Driver Max Queue Size: {}, Connection timeout: {}, Fetch Size: {}",
			// cluster.getClusterName(),
			// Cluster.getDriverVersion(),
			// cluster.getConfiguration().getPoolingOptions().getMaxQueueSize(),
			// cluster.getConfiguration().getSocketOptions().getConnectTimeoutMillis(),
			// fetchSize);

		} catch (Exception e) {
			e.printStackTrace();
			throw new CassandraException(e.getMessage());
		}
	}

	// @Override
	public Cluster getCluster() throws CassandraException {
		connectorLock.readLock().lock();
		try {
			if (cluster != null)
				return cluster;

			throw new CassandraException("The cluster is null at getCluster()");

		} finally {
			connectorLock.readLock().unlock();
		}
	}

	public Session getSession() throws CassandraException {
		connectorLock.readLock().lock();
		try {
			if (session != null)
				return session;

			throw new CassandraException("The session is null at getSession()");

		} finally {
			connectorLock.readLock().unlock();
		}
	}

	public MappingManager getMappingManager() throws CassandraException {
		connectorLock.readLock().lock();
		try {
			if (mappingManager != null)
				return mappingManager;

			throw new CassandraException("The Mapping Manager is null at getMappingManager()");

		} finally {
			connectorLock.readLock().unlock();
		}
	}

	public PreparedStatement getOrBuildPreparedStatementFromFactory(String query) throws CassandraException {
		// logger.finest(className, "getOrBuildPreparedStatementFromFactory({})",
		// query);

		PreparedStatement p = preparedStmtMap.get(query);
		if (p == null) {
			synchronized (mutexPrepare) {
				/*
				 * Double check after the lock to make sure another thread didn't add the
				 * prepared statement while locked
				 */
				p = preparedStmtMap.get(query);
				if (p == null) {
					try {
						// logger.fine(className, "prepare: {}", query);
						connectorLock.readLock().lock();
						try {
							preparedStmtMap.put(query, getSession().prepare(query));

						} finally {
							connectorLock.readLock().unlock();
						}

					} catch (Exception e) {
						e.printStackTrace();
						throw new CassandraException(e.getMessage());
					}
					p = preparedStmtMap.get(query);
				}
			}
		}

		return p;
	}

	public boolean addToQueue(StatementBean statement) {
		/* Stop adding to the queue once shutdown is declared */
		if (shutdown || statement == null)
			return false;

		final List<StatementBean> statements = new ArrayList<StatementBean>(1);
		statements.add((StatementBean) statement);

		return addToQueue(statements);
	}

	public boolean addToQueueBufferList(StatementBean statement) {
		if (shutdown || statement == null)
			return false;

		synchronized (bufferListMutex) {
			return bufferingStatements.add((StatementBean) statement);
		}
	}

	public boolean addToQueue(List<StatementBean> statement) {
		/* Stop adding to the queue once shutdown */
		if (shutdown || statement == null)
			return false;

		boolean complete;
		try {
			complete = queue.offer(statement);
			if (!complete)
				System.out.println("hi");
			// logger.warning(className, "addToQueue() Error: Could not add to the queue!-
			// Lost data");

		} catch (Exception e) {

			complete = false;
			// logger.warning(className, "addToQueue() Error: Could not add to the queue!-
			// Lost data- error message: {}",
			// e.getMessage());
			e.printStackTrace();
		}

		return complete;
	}

	public int getQueueSize() {
		return queue.size();
	}

	public boolean isQueueProcessing() {
		return getQueueSize() > 0 || queueProcessing;
	}

	/**
	 * Run the queue processor
	 * 
	 */
	public void queueRunner() {
		// logger.info(className, "Queue started");
		long startTime = 0;
		List<StatementBean> beans = null;

		/*
		 * Once shutdown, make sure the queue gets processed before exiting the thread
		 */
		while (!shutdown) {
			try {
				queueProcessing = false;
				beans = queue.take();
				queueProcessing = true;

				startTime = System.currentTimeMillis();
				// logger.finest(className, "Processing Queue...{} total", queue.size());

				if (beans.size() > maxBlockProcessor)
					multipleReads(beans);
				else
					fullListProcess(beans);

				// logger.finest(className, "Process completed in {}...{} left in queue",
				// new DateTimes().getFormattedDuration((System.currentTimeMillis() -
				// startTime)), queue.size());

				/* Check if the disconnection buffer has beans and process them */
				// checkDisconnectionBuffer();

			} catch (InterruptedException i) {
				;

			} catch (Exception e) {
				// logger.warning(className, e.getMessage());
			}

			if (!shutdown)
				handleBufferingList();
		}

		queueProcessing = false;
		// logger.info(className, "thread shutdown");
	}

	/**
	 * Used in conjunction with
	 * {@link CassandraConnectorService#addToQueueBufferList(StatementBean)
	 * addToQueueBufferList} to allow for a less costly process to send the list if
	 * available to
	 * 
	 */
	private void handleBufferingList() {
		if (bufferingStatements.size() == 0)
			return;

		synchronized (bufferListMutex) {
			if (bufferingStatements.size() > 0) {
				final List<StatementBean> statements = new ArrayList<StatementBean>(bufferingStatements);
				bufferingStatements = new ArrayList<StatementBean>();

				// logger.finest(className, "handleBufferingList() Offering {} statements to the
				// queue",
				// statements.size());
				addToQueue(statements);
			}
		}
	}

	/**
	 * Handle the processing of inserts greater than <code>maxBlockProcessor</code>
	 * 
	 * @param beans
	 * @throws Exception
	 */
	private void multipleReads(List<StatementBean> beans) {
		// logger.finer(className, "multipleReads(), Size: {}", beans.size());

		List<StatementBean> processBeans = new ArrayList<StatementBean>();

		for (int i = 1; i <= beans.size(); i++) {
			processBeans.add(beans.get(i - 1));

			if ((i == beans.size()) || (i % maxBlockProcessor) == 0) {
				fullListProcess(processBeans);
				if (i != beans.size())
					processBeans = new ArrayList<StatementBean>();
			}
		}
	}

	/**
	 * Handle the full process of the list (Less than or equal to
	 * <code>maxBlockProcessor</code>)
	 * 
	 * @param beans
	 * @throws Exception
	 */
	private void fullListProcess(List<StatementBean> beans) {
		// logger.finer(className, "fullListProcess(), Size: {}", beans.size());
		try {
			executeAsychInsertOrdered(beans);

		} catch (Exception e) {

			// logger.warning(className, "Exception during insert due to Error: {}; Saving
			// to disconnect buffer",
			// e.getMessage());
			/*
			 * If an Exception has been thrown, then we will *assume* that none of the list
			 * made it, so we will save the data within the database
			 */
			// addToDisconnectionBuffer(beans);
		}
	}

	/**
	 * Add the Statement beans that could not be inserted into the Nitrite
	 * disconnection buffer database for processing later
	 * 
	 * @param beans
	 */
	/*
	 * private void addToDisconnectionBuffer(List<StatementBean> beans) {
	 * disconnectionBufferLock.readLock().lock(); try { for (StatementBean bean :
	 * beans) { try { if (disconnectionBuffer != null)
	 * disconnectionBuffer.insert(bean); else throw new
	 * DisconnectBufferException("Disconnection Buffer was null at insert");
	 * 
	 * } catch (DisconnectBufferException e) { // logger.warning(className,
	 * "Lost bean persistance: {} Due to Error: {}", // beans.toString(), //
	 * e.getMessage()); } }
	 * 
	 * } finally { disconnectionBufferLock.readLock().unlock(); } }
	 */

	/**
	 * Check to see if there are any beans in the buffer and process them if there
	 * are
	 * 
	 */
	/*
	 * private void checkDisconnectionBuffer() { // logger.finest(className,
	 * "checkDisconnectionBuffer()");
	 * 
	 * disconnectionBufferLock.readLock().lock(); try { if (disconnectionBuffer !=
	 * null) { disconnectionBuffer.getCollections(); }
	 * 
	 * } catch (DisconnectBufferException e) { e.printStackTrace(); } finally {
	 * disconnectionBufferLock.readLock().unlock(); } }
	 */
	// @Override
	public ResultSet simpleStatementRS(String query) throws CassandraException {
		// logger.finest(className, "ResultSet simpleStatementRS({})", query);

		return getSession().execute(new SimpleStatement(query));
	}

	// @Override
	public Iterator<Row> simpleStatement(String query) throws CassandraException {
		// logger.finest(className, "Iterator<Row> simpleStatementRS({})", query);

		return getSession().execute(new SimpleStatement(query)).iterator();
	}

	// @Override
	public Iterator<Row> parameterStatement(String query, Object... values) throws CassandraException {
		// logger.finest(className, "parameterStatement({}, {})", query,
		// Arrays.toString(values));

		return getSession().execute(query, values).iterator();
	}

	// @Override
	public List<ResultSet> executeAsychInsertOrdered(List<StatementBean> statements) throws CassandraException {
		// logger.finest(className, "executeAsychInsertOrdered()");

		ListenableFuture<List<ResultSet>> futureList = null;

		try {
			futureList = queryAllInInsertOrder(statements);

		} catch (Exception e) {

			/* Try to cancel the other futures if an error occurs */
			if (futureList != null)
				futureList.cancel(true);

			while (!futureList.isDone()) {
				try {
					Thread.sleep(1);
				} catch (InterruptedException e1) {
				}
			}

			throw new CassandraException(e.getMessage());
		}

		try {
			return futureList.get();

		} catch (Exception e) {
			throw new CassandraException(e.getMessage());
		}
	}

//	@Override
	public List<ListenableFuture<ResultSet>> executeAsychCompletionOrdered(List<StatementBean> statements)
			throws CassandraException {

		// logger.finest(className, "executeAsychCompletionOrdered()");

		try {
			return queryAllInCompletionOrder(statements);
		} catch (Exception e) {
			throw new CassandraException(e.getMessage());
		}
	}

	/**
	 * Query all of the partitions and return a list of futures in the order that
	 * they were executed
	 * <p>
	 * 
	 * If you want to get the results as soon as they come in then use
	 * <code>queryAllInCompletionOrder()</code>
	 * 
	 * @param statements
	 * @return
	 * @throws Exception
	 */
	private ListenableFuture<List<ResultSet>> queryAllInInsertOrder(List<StatementBean> statements)
			throws CassandraException {
		// logger.finest(className, "queryAllInInsertOrder()");

		try {
			return Futures.allAsList(sendFutureQueries(statements));
		} catch (Exception e) {
			throw new CassandraException(e.getMessage());
		}
	}

	/**
	 * Query all of the partitions and return a list of futures in the order that
	 * they are completed
	 * 
	 * @param statements
	 * @return
	 * @throws Exception
	 */
	private List<ListenableFuture<ResultSet>> queryAllInCompletionOrder(List<StatementBean> statements)
			throws CassandraException {
//		logger.finest(className, "queryAllInCompletionOrder()");

		try {
			return Futures.inCompletionOrder(sendFutureQueries(statements));
		} catch (Exception e) {
			throw new CassandraException(e.getMessage());
		}
	}

	/**
	 * Send the prepared statement, bind the values and add them to the future list
	 * when executed
	 * 
	 * @param statements
	 * @return
	 * @throws Exception
	 */
	private List<ResultSetFuture> sendFutureQueries(List<StatementBean> statements) throws CassandraException {
		// logger.finest(className, "sendFutureQueries() size: {}", statements.size());
		BatchStatement batchStatement = new BatchStatement();
		final List<ResultSetFuture> futures = Lists.newArrayListWithCapacity(statements.size());
		for (StatementBean stmt : statements) {
			// logger.finest(className, "Stmt: {}", Arrays.toString(stmt.getValues()));
			PreparedStatement ps = getOrBuildPreparedStatementFromFactory(stmt.getPreparedStatement());

			Statement statement = new BoundStatement(ps).bind(stmt.getValues()).setFetchSize(fetchSize);
			batchStatement.add(statement);

		}

		//futures.add(getSession().executeAsync(batchStatement));
		getSession().execute(batchStatement);
		return futures;
	}

	/**
	 * Close the session and all connector associated resources
	 * 
	 * use <code>CassandraConnector.getInstance().close()</code>
	 * 
	 */
	private void close() {
		// logger.info(className, "close()");

		if (insertThread != null) {
			shutdown = true;
			insertThread.interrupt();

			try {
				insertThread.join();
			} catch (InterruptedException e) {
			}
		}

		connectorLock.writeLock().lock();
		try {
			if (session != null) {
				session.close();
				session = null;
			}

			if (cluster != null) {
				cluster.close();
				cluster = null;
			}

			mappingManager = null;

		} finally {
			connectorLock.writeLock().unlock();
		}
	}

}