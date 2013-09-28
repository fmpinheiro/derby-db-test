package com.devmonsters.derby;

import java.sql.*;

public class MainClient {

    //startar servidor: ij no diretorio do projeto
    public static void main(String[] args) throws Exception {
        final String driver = "org.apache.derby.jdbc.ClientDriver";

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
        System.out.println("Closing connection.");
    }

    private static Connection connectToDatabase(final String driver) throws ClassNotFoundException, SQLException {
        String connectionURL = "jdbc:derby://localhost:1527/devmonsters-derby-db-test";
        Class.forName(driver);
        return DriverManager.getConnection(connectionURL);
    }
}