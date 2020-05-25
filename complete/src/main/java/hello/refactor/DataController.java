package hello.refactor;

import hello.refactor.source.InfluxDBConnection;
import hello.refactor.source.IoTDBConnection;
import hello.refactor.source.PGConnection;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;
import java.sql.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.influxdb.dto.QueryResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
* 数据控制器，根据参数查询原始数据
*/
@RestController
public class DataController {

    private static Comparator<Map<String, Object>> sampleComparator = new Comparator<Map<String, Object>>(){
        @Override
        public int compare(Map<String, Object> sampleDataPoint1, Map<String, Object> sampleDataPoint2){
            long t1 = (Timestamp.valueOf(sampleDataPoint1.get("time").toString())).getTime();
            long t2 = (Timestamp.valueOf(sampleDataPoint2.get("time").toString())).getTime();
            return Math.round(t1-t2);
        }
    };

    @RequestMapping("/data")
    public List<Map<String, Object>> dataPoints(
            @RequestParam(value="url", defaultValue = "jdbc:iotdb://127.0.0.1:6667/") String url,
            @RequestParam(value="username", defaultValue = "root") String username,
            @RequestParam(value="password", defaultValue = "root") String password,
            @RequestParam(value="database") String database,
            @RequestParam(value="timeseries", required = false) String timeseries,
            @RequestParam(value="columns", required = false) String columns,
            @RequestParam(value="starttime", required = false) String starttime,
            @RequestParam(value="endtime", required = false) String endtime,
            @RequestParam(value="conditions", required = false) String conditions,
            @RequestParam(value="query", required = false) String query,
            @RequestParam(value="format", defaultValue = "map") String format,
            @RequestParam(value="ip", required = false) String ip,
            @RequestParam(value="port", required = false) String port,
            @RequestParam(value="amount", required = false) Integer amount,
            @RequestParam(value="dbtype", defaultValue = "iotdb") String dbtype
    ) throws SQLException {
        // trim the '"' of the parameters
        url = url.replace("\"", "");
        username = username.replace("\"", "");
        password = password.replace("\"", "");
        database = database.replace("\"", "");
        timeseries = timeseries.replace("\"", "");
        columns = columns.replace("\"", "");
        starttime = starttime == null ? null : starttime.replace("\"", "");
        endtime = endtime == null ? null :endtime.replace("\"", "");
        conditions = conditions == null ? null : conditions.replace("\"", "");
        format = format.replace("\"", "");
        dbtype = dbtype.replace("\"", "");
        ip = ip == null ? null : ip.replace("\"", "");
        port = port == null ? null : port.replace("\"", "");
        query = query == null ? null : query.replace("\"", "");

        return _dataPoints(url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, format, ip, port, dbtype);
    }

    public static List<Map<String, Object>> _dataPoints(
            String url,
            String username,
            String password,
            String database,
            String timeseries,
            String columns,
            String starttime,
            String endtime,
            String conditions,
            String query,
            String format,
            String ip,
            String port,
            String dbtype
    ) throws SQLException {

        List<Map<String, Object>> res = new LinkedList<>();
        System.out.println(dbtype);

        if(dbtype.toLowerCase().equals("iotdb")){
            if(ip != null && port != null) url = String.format("jdbc:iotdb://%s:%s/", ip, port);

            Connection connection = IoTDBConnection.getConnection(url, username, password);
            if (connection == null) {
                System.out.println("get connection defeat");
                return res;
            }
            Statement statement = connection.createStatement();
            String sql = query != null ? query :
                    "SELECT " + columns +
                    " FROM " + database + "." + timeseries +
                    (starttime == null ? "" : " WHERE time >= " + starttime) +
                    (endtime   == null ? "" : " AND time < " + endtime) +
                    (conditions == null ? "" : conditions);
            System.out.println(sql);
            ResultSet resultSet = statement.executeQuery(sql);

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
                        else if(Types.DATE == type) {
                            map.put(label.toLowerCase(), resultSet.getDate(i));
                            map.put("timestamp", resultSet.getDate(i).getTime());
                        }
                        else if(Types.TIME == type) {
                            map.put(label.toLowerCase(), resultSet.getTime(i));
                            map.put("timestamp", resultSet.getTime(i).getTime());
                        }
                        else if(Types.TIMESTAMP == type) {
                            map.put(label.toLowerCase(), resultSet.getTimestamp(i));
                            map.put("timestamp", resultSet.getTimestamp(i).getTime());
                        }
                        else map.put(label, resultSet.getString(i));
                    }
                    res.add(map);
                }
            }
            statement.close();
            connection.close();
        }
        else if(dbtype.toLowerCase().equals("pg")){
            if(ip != null && port != null) url = String.format("jdbc:postgresql://%s:%s/", ip, port);

            PGConnection pgtool = new PGConnection(url+database, username, password);
            Connection connection = pgtool.getConn();
            if (connection == null) {
                System.out.println("get connection defeat");
                return res;
            }
            String sql = query != null ? query :
                    "SELECT " + "time, " + columns +
                    " FROM " + timeseries +
                    (starttime == null ? "" : " WHERE time >= '" + starttime + "'") +
                    (endtime   == null ? "" : " AND time < '" + endtime + "'") +
                    (conditions == null ? "" : " " + conditions);
            System.out.println(sql);
            ResultSet resultSet = pgtool.query(connection, sql);

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
                        else if(Types.NUMERIC == type) map.put(label, resultSet.getDouble(i));
                        else if(Types.DATE == type) {
                            map.put(label.toLowerCase(), resultSet.getDate(i));
                            map.put("timestamp", resultSet.getDate(i).getTime());
                        }
                        else if(Types.TIME == type) {
                            map.put(label.toLowerCase(), resultSet.getTime(i));
                            map.put("timestamp", resultSet.getTime(i).getTime());
                        }
                        else if(Types.TIMESTAMP == type) {
                            map.put(label.toLowerCase(), resultSet.getTimestamp(i));
                            map.put("timestamp", resultSet.getTimestamp(i).getTime());
                        }
                        else map.put(label, resultSet.getString(i));
                    }
                    res.add(map);
                }
            }
            connection.close();
        }
        else if(dbtype.toLowerCase().equals("influxdb")){
            if(ip != null && port != null) url = String.format("http://%s:%s/", ip, port);

            InfluxDBConnection influxDBConnection = new InfluxDBConnection(url, username, password, database, null);
            String sql = query != null ? query :
                    "SELECT " + columns +
                    " FROM " + timeseries +
                    (starttime == null ? "" : " WHERE time >= " + starttime) +
                    (endtime   == null ? "" : " AND time < " + endtime) +
                    (conditions == null ? "" : conditions);
            System.out.println(sql);
            QueryResult queryResult = influxDBConnection.query(sql);

            for(QueryResult.Result result : queryResult.getResults()){
                for(QueryResult.Series series : result.getSeries()){
                    List<String> cols = series.getColumns();
                    for(List<Object> objects :series.getValues()){
                        Map<String, Object> map = new HashMap<>();
                        for(int i = 0; i < objects.size(); i++){
                            if(cols.get(i).equals("time")) {
                                map.put(cols.get(i), objects.get(i).toString().replace("T", " ").replace("Z", ""));
                            }
                            else map.put(cols.get(i), objects.get(i));
                        }
                        res.add(map);
                    }
                }
            }
            influxDBConnection.close();
        }
        else if(dbtype.equals("kafka")){

            // TODO: Kafka数据模型定义尚不完善，目前仅测试版本
            System.out.println(url);
            System.out.println(timeseries);
            System.out.println(columns);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

            Properties props = new Properties();
            props.put("bootstrap.servers", url);
            props.put("group.id", "fast");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.DoubleDeserializer");
            props.put("auto.offset.reset","earliest");
            props.put("max.poll.records", "10000");

            KafkaConsumer<Long, Double> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(timeseries));
            ConsumerRecords<Long, Double> records = consumer.poll(Duration.ofMillis(100));
            System.out.println(records.count());
            for (ConsumerRecord<Long, Double> record : records) {
                Map<String, Object> map = new HashMap<>();
                map.put("timestamp", record.key());
                map.put("time", sdf.format(record.key()).substring(0,23));
                map.put(columns.toLowerCase(), record.value());
                res.add(map);
            }
            consumer.close();
        }
        else return res;

        // 部分数据源存在查询结果时间乱序问题
        res.sort(sampleComparator);

        if(format.equals("map")) return res;
        List<Map<String, Object>> result = new LinkedList<>();
        for(Map<String, Object> map : res){
            Object time = map.get("time");
            for(Map.Entry<String, Object> entry : map.entrySet()){
                String mapKey = entry.getKey();
                if(mapKey.equals("time")) continue;
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
