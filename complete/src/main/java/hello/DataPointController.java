package hello;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;
import org.apache.iotdb.jdbc.IoTDBSQLException;

@RestController
public class DataPointController {

    @RequestMapping("/data")
    public List<Map<String, String>> timeSeries(
            @RequestParam(value="database") String database,
            @RequestParam(value="timeseries") String timeseries,
            @RequestParam(value="columns") String columns,
            @RequestParam(value="starttime", required = false) Long starttime,
            @RequestParam(value="endtime", required = false) Long endtime,
            @RequestParam(value="conditions", required = false) String conditions
        ) throws SQLException {
        Connection connection = IoTDBConnection.getConnection();
        if (connection == null) {
            System.out.println("get connection defeat");
            return null;
        }
        long stime = System.currentTimeMillis();
        Statement statement = connection.createStatement();
        String sql = "SELECT " + columns.replace("\"", "") +
                     " FROM " + database.replace("\"", "") + "." + timeseries.replace("\"", "") +
                     (starttime == null ? "" : " WHERE time >= " + starttime) +
                     (endtime   == null ? "" : " AND time <= " + endtime) +
                     (conditions    == null ? "" : " AND " + conditions.replace("\"", ""));
        ResultSet resultSet = statement.executeQuery(sql);
        List<Map<String, String>> res = new LinkedList<>();
        if (resultSet != null) {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, String> map = new HashMap<>();
                for(int i = 1; i <= columnCount; i++){
                    map.put(metaData.getColumnLabel(i), resultSet.getString(i));
                }
                res.add(map);
            }
        }
        statement.close();
        connection.close();
        stime = System.currentTimeMillis() - stime;
        System.out.println("used time: " + stime + "ms");
        return res;
    }
}
