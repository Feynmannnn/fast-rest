package hello;

import java.util.LinkedList;
import java.util.List;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;
import org.apache.iotdb.jdbc.IoTDBSQLException;

@RestController
public class StorageGroupController {
    @RequestMapping("/storageGroup")
    public List<StorageGroup> storageGroup() throws SQLException {
        Connection connection = IoTDBConnection.getConnection();
        if (connection == null) {
            System.out.println("get connection defeat");
            return null;
        }
        Statement statement = connection.createStatement();
        String sql = "SHOW STORAGE GROUP";
        statement.execute(sql);
        ResultSet resultSet = statement.getResultSet();
        List<StorageGroup> storageGroup = new LinkedList<>();
        if (resultSet != null) {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                storageGroup.add(new StorageGroup(resultSet.getString(1)));
            }
        }
        statement.close();
        connection.close();
        return storageGroup;
    }
}
