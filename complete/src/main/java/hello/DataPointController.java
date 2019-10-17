package hello;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;
import org.apache.iotdb.jdbc.IoTDBSQLException;

@RestController
public class DataPointController {

    @RequestMapping("/data")
    public List<Map<String, String>> timeSeries(
            @RequestParam(value="timeseries") String timeseries,
            @RequestParam(value="columns") String columns,
            @RequestParam(value="starttime", required = false) Long starttime,
            @RequestParam(value="endtime", required = false) Long endtime,
            @RequestParam(value="filter", required = false) String filter,
            @RequestParam(value="limit", required = false) Integer limit,
            @RequestParam(value="offset", required = false) Integer offset
        ) throws SQLException {
        Connection connection = IoTDBConnection.getConnection();
        if (connection == null) {
            System.out.println("get connection defeat");
            return null;
        }
        Statement statement = connection.createStatement();
        String sql = "SELECT " + columns.replace('"', ' ') +
                     " FROM " + timeseries.replace('"', ' ') +
                     (starttime == null ? "" : " WHERE time > " + starttime) +
                     (endtime   == null ? "" : " AND time < " + endtime) +
                     (filter    == null ? "" : " AND " + filter.replace('"', ' ')) +
                     (limit     == null ? "" : " LIMIT " + limit) +
                     (offset    == null ? "" : " OFFSET " + offset);
        ResultSet resultSet = statement.executeQuery(sql);
        List<DataPoint> dataPoints = new LinkedList<>();
        List<Map<String, String>> res = new LinkedList<>();
        if (resultSet != null) {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                dataPoints.add(new DataPoint(
                        resultSet.getLong(1),
                        resultSet.getString(2)
                ));
                Map<String, String> map = new HashMap<>();
                for(int i = 1; i <= columnCount; i++){
                    map.put(metaData.getColumnLabel(i), resultSet.getString(i));
                }
                res.add(map);
            }
        }
        statement.close();
        connection.close();
        return res;
    }
}
