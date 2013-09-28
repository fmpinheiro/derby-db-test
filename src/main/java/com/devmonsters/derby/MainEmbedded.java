package com.devmonsters.derby;

import java.sql.*;

public class MainEmbedded {

    public static void main(String[] args) throws Exception {
        final String driver = "org.apache.derby.jdbc.EmbeddedDriver";

        System.out.println(String.format("Connecting to database using %s driver", driver));
        Connection connection = connectToDatabase(driver);
        System.out.println(String.format("Connected! Got %s connection object", connection));
        System.out.println("Listing system tables");
        try (PreparedStatement pstmt = connection.prepareStatement("SELECT * FROM SYS.SYSTABLES ORDER BY TABLEID"); ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                System.out.println(String.format("Table ID: %s, Table name: %s, Table type: %s", rs.getString("TABLEID"), rs.getString("TABLENAME"), rs.getString("TABLETYPE")));
            }
        } finally {
            connection.close();
        }
        System.out.println("Closing connection and shutting down the database.");

        boolean didShutdown = shutdownDatabaseIfEmbedded(driver);
        if (!didShutdown) {
            System.out.println("Database did not shut down normally or it is not embedded");
        } else {
            System.out.println("Database shut down normally");
        }

    }

    private static Connection connectToDatabase(final String driver) throws ClassNotFoundException, SQLException {
        String connectionURL = "jdbc:derby:devmonsters-derby-db-test;create=true"; //;create=true nao e necessario para apos a criacao
        Class.forName(driver);
        return DriverManager.getConnection(connectionURL);
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