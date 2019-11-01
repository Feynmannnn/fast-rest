package hello;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;
import org.apache.iotdb.jdbc.IoTDBSQLException;

@RestController
public class ColumnController {
    @RequestMapping("/columns")
    public List<Column> columns(
            @RequestParam(value="database", defaultValue="root") String database,
            @RequestParam(value="timeseries", defaultValue="root") String timeseries
    ) throws SQLException {
        Connection connection = IoTDBConnection.getConnection();
        if (connection == null) {
            System.out.println("get connection defeat");
            return null;
        }
        Statement statement = connection.createStatement();
        String sql = "SHOW TIMESERIES " +
                database.replace("\"", "") + "." +
                timeseries.replace("\"", "");
        statement.execute(sql);
        ResultSet resultSet = statement.getResultSet();
        List<Column> columns = new LinkedList<>();
        HashSet<String> set = new HashSet<>();
        if (resultSet != null) {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                String column = resultSet.getString(1).split("\\.")[3];
                if(!set.contains(column)){
                    columns.add(new Column(column));
                    set.add(column);
                }
            }
        }
        statement.close();
        connection.close();
        return columns;
    }
}
