package com.devmonsters.derby;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MainClient {
	public static void main(String[] args) throws Exception {
		final String driver = "org.apache.derby.jdbc.ClientDriver";

		System.out.println(String.format(
				"Connecting to database using %s driver", driver));
		Connection connection = connectToDatabase(driver);
		System.out.println(String.format("Connected! Got %s connection object",
				connection));
		try {
			System.out.println("Listing system tables");

			PreparedStatement pstmt = connection
					.prepareStatement("SELECT * FROM SYS.SYSTABLES ORDER BY TABLEID");
			try {
				ResultSet rs = pstmt.executeQuery();
				try {
					while (rs.next()) {
						System.out.println(String.format(
								"Table ID: %s, Table name: %s, Table type: %s",
								rs.getString("TABLEID"),
								rs.getString("TABLENAME"),
								rs.getString("TABLETYPE")));
					}
				} finally {
					rs.close();
				}
			} finally {
				pstmt.close();
			}
		} finally {
			connection.close();
		}
		System.out
				.println("Closing connection and shutting down the database.");
		boolean didShutdown = shutdownDatabaseIfEmbedded(driver);

		if (!didShutdown) {
			System.out.println("Database did not shut down normally or it is not embedded");
		} else {
			System.out.println("Database shut down normally");
		}

	}

	private static Connection connectToDatabase(final String driver)
			throws ClassNotFoundException, SQLException {
		String connectionURL = "jdbc:derby://localhost:1527/devmonsters-derby-db-test";
		Class.forName(driver);
		Connection conn = DriverManager.getConnection(connectionURL);
		return conn;
	}

	private static boolean shutdownDatabaseIfEmbedded(String driver) {
		if (driver.equals("org.apache.derby.jdbc.EmbeddedDriver")) {
			try {
				DriverManager.getConnection("jdbc:derby:;shutdown=true");
			} catch (SQLException se) {
				if (se.getSQLState().equals("XJ015")) {
					return true;
				}
			}
		}
		return false;
	}
}