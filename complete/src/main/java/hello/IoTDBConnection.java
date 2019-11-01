package hello;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class IoTDBConnection {
    static Connection getConnection() {
        // JDBC driver name and database URL
        String driver = "org.apache.iotdb.jdbc.IoTDBDriver";
        String url = "jdbc:iotdb://101.6.15.218:6667/";

        // Database credentials
        String username = "root";
        String password = "root";

        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static Statement getStatement() throws SQLException {
        return getConnection().createStatement();
    }
}
