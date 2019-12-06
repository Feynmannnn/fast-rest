package hello;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.util.DigestUtils;

import java.sql.*;
import java.util.Map;

import org.apache.iotdb.jdbc.IoTDBSQLException;

@RestController
public class PublishController {

    private static final String slat = "&%333***&&%%$$#@";

    @RequestMapping("/publish")
    public List<Map<String, Object>> publish(
            @RequestParam(value="url", defaultValue = "jdbc:iotdb://127.0.0.1:6667/") String url,
            @RequestParam(value="username", defaultValue = "root") String username,
            @RequestParam(value="password", defaultValue = "root") String password,
            @RequestParam(value="database") String database,
            @RequestParam(value="timeseries") String timeseries,
            @RequestParam(value="columns") String columns,
            @RequestParam(value="starttime", required = false) String starttime,
            @RequestParam(value="endtime", required = false) String endtime,
            @RequestParam(value="format", defaultValue = "map") String format
    ) throws SQLException {
        Connection connection = IoTDBConnection.getConnection(url, username, password);
        if (connection == null) {
            System.out.println("get connection defeat");
            return null;
        }
        long stime = System.currentTimeMillis();
        Statement statement = connection.createStatement();
        String sql = "SELECT " + columns.replace("\"", "") +
                " FROM " + database.replace("\"", "") + "." + timeseries.replace("\"", "") +
                (starttime == null ? "" : " WHERE time >= " + starttime.replace("\"", "")) +
                (endtime   == null ? "" : " AND time <= " + endtime.replace("\"", ""));
        ResultSet resultSet = statement.executeQuery(sql);
        System.out.println("exec used time: " + (System.currentTimeMillis() - stime) + "ms");
        List<Map<String, Object>> res = new LinkedList<>();
        if (resultSet != null) {
            final ResultSetMetaData metaData = resultSet.getMetaData();
            final int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                for(int i = 1; i <= columnCount; i++){
                    int type = metaData.getColumnType(i);
                    String label = metaData.getColumnLabel(i);
                    if(Types.INTEGER == type) map.put(label, resultSet.getInt(i));
                    else if(Types.BIGINT == type) map.put(label, resultSet.getLong(i));
                    else if(Types.BOOLEAN == type) map.put(label, resultSet.getString(i));
                    else if(Types.FLOAT == type) map.put(label, resultSet.getFloat(i));
                    else if(Types.DOUBLE == type) map.put(label, resultSet.getDouble(i));
                    else if(Types.DATE == type) map.put(label, resultSet.getDate(i));
                    else if(Types.TIME == type) map.put(label, resultSet.getTime(i));
                    else if(Types.TIMESTAMP == type) map.put(label, resultSet.getTimestamp(i));
                    else map.put(label, resultSet.getString(i));
                }
                res.add(map);
            }
        }
        statement.close();
        connection.close();
        System.out.println("publish used time: " + (System.currentTimeMillis() - stime) + "ms");
        if(format.equals("map")) return res;
        List<Map<String, Object>> result = new LinkedList<>();
        for(Map<String, Object> map : res){
            Object time = map.get("Time");
            for(Map.Entry<String, Object> entry : map.entrySet()){
                String mapKey = entry.getKey();
                if(mapKey.equals("Time")) continue;
                Map<String, Object> m = new HashMap<>();
                m.put("time", time);
                m.put("label", mapKey);
                m.put("value", entry.getValue());
                result.add(m);
            }
        }
        return result;
    }
}
