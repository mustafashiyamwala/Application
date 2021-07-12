package com.org.service;

import org.apache.log4j.Logger;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.mapping.Mapper;
import com.org.beans.BBData;
import com.org.exception.CassandraException;

public class BBService {

	private Session session;
	private String keySpace;
	private Mapper<BBData> mapper;
	private static Logger logger = Logger.getLogger(BBService.class);

	public BBService(Session session, String keySpace, Mapper<BBData> mapper) {
		this.session = session;
		this.keySpace = keySpace;
		this.mapper = mapper;
	}

	public long sendMessage(BBData message) throws CassandraException {
		try {
			this.mapper.save(message);
			return 0;

		} catch (Exception e) {
			logger.error("Exception: " + e.getMessage());
			throw new CassandraException("Exception: " + e.getMessage());
		}
	}

	public Iterable<BBData> getMessages(String recipient) throws CassandraException {
		try {
			String retrieveQuery = "SELECT * FROM BBData";
			return this.mapper.map(session.execute(retrieveQuery));

		} catch (Exception e) {
			logger.error("Exception: " + e.getMessage());
			throw new CassandraException("Exception: " + e.getMessage());
		}
	}
}
