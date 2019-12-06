package hello;

import java.security.NoSuchAlgorithmException;
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
public class SubscribeController {

    private static final String slat = "&%12345***&&%%$$#@1";

    @RequestMapping("/subscribe")
    public String subscribe(
            @RequestParam(value="url", defaultValue = "jdbc:iotdb://127.0.0.1:6667/") String url,
            @RequestParam(value="username", defaultValue = "root") String username,
            @RequestParam(value="password", defaultValue = "root") String password,
            @RequestParam(value="database") String database,
            @RequestParam(value="timeseries") String timeseries,
            @RequestParam(value="columns") String columns,
            @RequestParam(value="starttime") String starttime,
            @RequestParam(value="theta", defaultValue = "50") Integer theta,
            @RequestParam(value="k", defaultValue = "4") Integer k
    ) throws SQLException, NoSuchAlgorithmException {

        url = url.replace("\"", "");
        username = username.replace("\"", "");
        password = password.replace("\"", "");
        database = database.replace("\"", "");
        timeseries = timeseries.replace("\"", "");
        columns = columns.replace("\"", "");
        starttime = starttime.replace("\"", "");

        String Identifier = String.format("%s,%s,%s,%s,%s,%s,%s", url, database, timeseries, columns, theta, k, slat);
        String subId = "M" + DigestUtils.md5DigestAsHex(Identifier.getBytes()).substring(0,8);
        System.out.println(subId);

        SubscribeThread subscribeThread = new SubscribeThread(url, username, password, database, timeseries, columns, starttime, theta, k, subId);
        subscribeThread.start();

//        // TODO: analyse column type
//        String TYPE = "DOUBLE";
//        // TODO: analyse encoding type
//        String ENCODING = "GORILLA";
//
//        Connection connection = IoTDBConnection.getConnection(url, username, password);
//        if (connection == null) {
//            System.out.println("get connection defeat");
//            return null;
//        }
//        Statement statement = connection.createStatement();
//        String deletesql = String.format("delete from %s.%s.%s where time>=%s", database, subId, columns, starttime);
//        String sql = String.format("create timeseries %s.%s.%s with datatype=%s, encoding=%s", database, subId, columns, TYPE, ENCODING);
//
//        // TODO: check if the subId exists
//        try {
//            statement.execute(sql);
//        }catch (IoTDBSQLException e){
//            System.out.println(e.getMessage());
//        }
//
//        List<Map<String, Object>> sampleDataPoints = new BucketSampleController().dataPoints(
//                url, username, password, database, timeseries, columns, starttime, null, null, theta, k, "map");
//
//        long stime = System.currentTimeMillis();
//        String label = database.replace("\"", "") + "." + timeseries.replace("\"", "") + "." + columns.replace("\"", "");
//        String batchInsertFormat = "insert into %s.%s(timestamp, %s) values(%s, %s);";
//
//        for(Map<String, Object> map : sampleDataPoints){
//            statement.addBatch(String.format(batchInsertFormat, database, subId, columns, map.get("Time").toString().substring(0,19), map.get(label)));
//        }
//        statement.executeBatch();
//        statement.clearBatch();
//
//        System.out.println("batch inserts used time: " + (System.currentTimeMillis() - stime) + "ms");
        return subId;
    }
}
