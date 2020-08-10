package com.org.bundle;

import java.util.Hashtable;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.datastax.driver.mapping.MappingManager;
import com.org.beans.BBData;
import com.org.service.BBService;

import io.netty.util.ThreadDeathWatcher;

public class CassandraActivator implements BundleActivator {

	private String[] urls;
	private Integer port;
	private String userName;
	private String password;
	private String keySpace;
	private String tableName;
	private Session session;
	private Cluster cluster;
	private static Logger logger = Logger.getLogger(CassandraActivator.class);

	public CassandraActivator(String[] urls, Integer port, String userName, String password, String keySpace,
			String tableName) {
		this.urls = urls;
		this.port = port;
		this.userName = userName;
		this.password = password;
		this.keySpace = Metadata.quote(keySpace);
		this.tableName = tableName;
	}

	@Override
	public void start(BundleContext context) throws Exception {
		// TODO Auto-generated method stub

		logger.info("Starting bundle {}" + context.getBundle().getSymbolicName());
		this.cluster = Cluster.builder().addContactPoints(this.urls).withPort(this.port)
				.withAuthProvider(new PlainTextAuthProvider(this.userName, this.password)).build();
		this.session = cluster.connect(keySpace);

		logger.info("Cassandra Connection is Done");

		MappingManager mappingManager = new MappingManager(this.session);
		Mapper<BBData> mapper = mappingManager.mapper(BBData.class);

		//BBService bbService = new BBService(this.session, this.keySpace, mapper);
		
		context.registerService(BBService.class.getName(), context.getBundle(), new Hashtable<String, String>());

		logger.info("Bundle {} successfully started" + context.getBundle().getSymbolicName());
	}

	@Override
	public void stop(BundleContext context) throws Exception {
		// TODO Auto-generated method stub
		logger.info("Stopping bundle {}" + context.getBundle().getSymbolicName());

		if (cluster != null) {
			cluster.close();
			// Await for Netty threads to stop gracefully
			ThreadDeathWatcher.awaitInactivity(10, TimeUnit.SECONDS);
		}

		logger.info("Bundle {} successfully stopped" + context.getBundle().getSymbolicName());
	}

}
