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
public class TimeSeriesController {

    @RequestMapping("/timeseries")
    public List<TimeSeries> timeSeries(@RequestParam(value="database", defaultValue="root") String database) throws SQLException {
        Connection connection = IoTDBConnection.getConnection();
        if (connection == null) {
            System.out.println("get connection defeat");
            return null;
        }
        Statement statement = connection.createStatement();
        String sql = "SHOW TIMESERIES " + database.replace("\"", "");
        statement.execute(sql);
        ResultSet resultSet = statement.getResultSet();
        List<TimeSeries> timeSeries = new LinkedList<>();
        HashSet<String> set = new HashSet<>();
        if (resultSet != null) {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                String device = resultSet.getString(1).split("\\.")[2];
                if(!set.contains(device)){
                    timeSeries.add(new TimeSeries(device));
                    set.add(device);
                }
            }
        }
        statement.close();
        connection.close();
        return timeSeries;
    }
}
