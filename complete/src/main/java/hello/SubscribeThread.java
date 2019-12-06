package hello;

import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.springframework.util.DigestUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

public class SubscribeThread extends Thread {

    String url;
    String username;
    String password;
    String database;
    String timeseries;
    String columns;
    String starttime;
    Integer theta;
    Integer k;
    String subId;

    SubscribeThread(String url, String username, String password, String database, String timeseries, String columns, String starttime, Integer theta, Integer k, String subId){
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
        this.timeseries = timeseries;
        this.columns = columns;
        this.starttime = starttime;
        this.theta = theta;
        this.k = k;
        this.subId = subId;
    }

    @Override
    public void run() {
        long stime = System.currentTimeMillis();

        // TODO: analyse column type
        String TYPE = "DOUBLE";
        // TODO: analyse encoding type
        String ENCODING = "GORILLA";

//        Connection connection = IoTDBConnection.getConnection(url, username, password);
//        if (connection == null) {
//            System.out.println("get connection defeat");
//            return;
//        }
//        Statement statement = null;
//        try {
//            statement = connection.createStatement();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        String deletesql = String.format("delete from %s.%s.%s where time>=%s", database, subId, columns, starttime);
//        String sql = String.format("create timeseries %s.%s.%s with datatype=%s, encoding=%s", database, subId, columns, TYPE, ENCODING);
//
//        // TODO: check if the subId exists
//        try {
//            statement.execute(sql);
//        }catch (IoTDBSQLException e){
//            System.out.println(e.getMessage());
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//
//        List<Map<String, Object>> sampleDataPoints = null;
//        try {
//            sampleDataPoints = new BucketSampleController().dataPoints(
//                    url, username, password, database, timeseries, columns, starttime, null, null, theta, k, "map");
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//
//        String label = database.replace("\"", "") + "." + timeseries.replace("\"", "") + "." + columns.replace("\"", "");
//        String batchInsertFormat = "insert into %s.%s(timestamp, %s) values(%s, %s);";
//
//        for(Map<String, Object> map : sampleDataPoints){
//            try {
//                statement.addBatch(String.format(batchInsertFormat, database, subId, columns, map.get("Time").toString().substring(0,19), map.get(label)));
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//        }
//        try {
//            statement.executeBatch();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        try {
//            statement.clearBatch();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }

        System.out.println("batch inserts used time: " + (System.currentTimeMillis() - stime) + "ms");
    }
}
