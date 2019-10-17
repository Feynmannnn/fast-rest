package hello;

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
    public List<TimeSeries> timeSeries(@RequestParam(value="path", defaultValue="root") String path) throws SQLException {
        Connection connection = IoTDBConnection.getConnection();
        if (connection == null) {
            System.out.println("get connection defeat");
            return null;
        }
        Statement statement = connection.createStatement();
        String sql = "SHOW TIMESERIES " + path.replace('-', '.');
        statement.execute(sql);
        ResultSet resultSet = statement.getResultSet();
        List<TimeSeries> timeSeries = new LinkedList<>();
        if (resultSet != null) {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                timeSeries.add(new TimeSeries(
                        resultSet.getString(1),
                        resultSet.getString(2),
                        resultSet.getString(3),
                        resultSet.getString(4))
                );
            }
        }
        statement.close();
        connection.close();
        return timeSeries;
    }
}
