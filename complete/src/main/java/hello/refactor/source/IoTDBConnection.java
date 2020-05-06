package hello.refactor.source;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class IoTDBConnection {
    public static Connection getConnection(String url, String username, String password) {
        // JDBC driver name and database URL
        String driver = "org.apache.iotdb.jdbc.IoTDBDriver";

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
