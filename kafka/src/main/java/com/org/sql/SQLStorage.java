package com.org.sql;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import lombok.Data;

@Data
public class SQLStorage implements Closeable {

	private Connection connection;
	private Statement statement;

	public SQLStorage() {
		sqlConnection();
	}

	public void sqlConnection() {
		try {
			Class.forName("com.mysql.jdbc.Driver");
			this.connection = DriverManager.getConnection("jdbc:mysql//localhost:3306/testdb", "root", "12345");
			this.connection.setAutoCommit(false);

			this.statement = this.connection.createStatement();

		} catch (ClassNotFoundException | SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("Exception: " + e.getMessage());
		}
	}

	public ResultSet pullRecords(String sql) {

		ResultSet resultSet = null;
		try {
			resultSet = this.statement.executeQuery(sql);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("Exception: " + e.getMessage());
		}
		return resultSet;
	}

	public int pushRecords(String sql) {

		int status = 0;
		try {
			status = this.statement.executeUpdate(sql);

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("Exception: " + e.getMessage());
		}
		return status;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		try {
			this.statement.close();
			this.connection.close();

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("Exception: " + e.getMessage());
		}
	}
}
