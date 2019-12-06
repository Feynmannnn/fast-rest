package hello;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class IoTDBConnection {
    static Connection getConnection(String url, String username, String password) {
        // JDBC driver name and database URL
        String driver = "org.apache.iotdb.jdbc.IoTDBDriver";
//        String url = "jdbc:iotdb://172.16.244.9:6667/";
//        String url = "jdbc:iotdb://101.6.15.203:6667/";
//        String url = "jdbc:iotdb://127.0.0.1:6667/";

        // Database credentials
//        String username = "root";
//        String password = "root";

        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(
                    url.replace("\"", ""),
                    username.replace("\"", ""),
                    password.replace("\"", "")
            );
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }
}
