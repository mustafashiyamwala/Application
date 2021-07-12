package com.org.utils;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.Logger;

import com.org.exception.CassandraException;

public class CSVUtility {

	private static Logger logger = Logger.getLogger(CSVUtility.class);

	public Iterable<CSVRecord> readCSVFile(String path) throws CassandraException {
		Iterable<CSVRecord> iterable = null;

		try {
			Reader reader = new FileReader(path);
			iterable = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader);
			logger.info("CSV File " + path + " Parse Successfully");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			logger.error("Exception: " + e.getMessage());
			throw new CassandraException("Exception: " + e.getMessage());
		}
		return iterable;
	}
}
