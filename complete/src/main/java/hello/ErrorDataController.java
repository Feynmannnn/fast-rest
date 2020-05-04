package hello;

import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.influxdb.dto.QueryResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;
import org.apache.iotdb.jdbc.IoTDBSQLException;

@RestController
public class ErrorDataController {

    private static Comparator<Map<String, Object>> sampleComparator = new Comparator<Map<String, Object>>(){
        @Override
        public int compare(Map<String, Object> sampleDataPoint1, Map<String, Object> sampleDataPoint2){
            long t1 = (Timestamp.valueOf(sampleDataPoint1.get("time").toString())).getTime();
            long t2 = (Timestamp.valueOf(sampleDataPoint2.get("time").toString())).getTime();
            return Math.round(t1-t2);
        }
    };

    @RequestMapping("/errordata")
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

        List<Map<String, Object>> res = new LinkedList<>();

        System.out.println(dbtype);

        if(dbtype.toLowerCase().equals("iotdb")){
            if(ip != null && port != null) url = String.format("jdbc:iotdb://%s:%s/", ip, port);

            Connection connection = IoTDBConnection.getConnection(url, username, password);
            if (connection == null) {
                System.out.println("get connection defeat");
                return null;
            }
            long stime = System.currentTimeMillis();
            Statement statement = connection.createStatement();
            String sql = query != null ? query :
                    "SELECT " + columns +
                    " FROM " + database + "." + timeseries +
                    (starttime == null ? "" : " WHERE time >= " + starttime) +
                    (endtime   == null ? "" : " AND time < " + endtime) +
                    (conditions    == null ? "" : conditions);
            System.out.println(sql);
            ResultSet resultSet = statement.executeQuery(sql);
            System.out.println("exec used time: " + (System.currentTimeMillis() - stime) + "ms");

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
                        else if(Types.TIME == type) map.put(label.toLowerCase(), resultSet.getTime(i));
                        else if(Types.TIMESTAMP == type) map.put(label.toLowerCase(), resultSet.getTimestamp(i));
                        else map.put(label, resultSet.getString(i));
                    }
                    res.add(map);
                }
            }
            statement.close();
            connection.close();
            System.out.println("used time: " + (System.currentTimeMillis() - stime) + "ms");
        }
        else if(dbtype.toLowerCase().equals("pg")){
            if(ip != null && port != null) url = String.format("jdbc:postgresql://%s:%s/", ip, port);

            PGConnection pgtool = new PGConnection(url+database, username, password);
            Connection connection = pgtool.getConn();
            if (connection == null) {
                System.out.println("get connection defeat");
                return null;
            }
            String sql = query != null ? query :
                    "SELECT " + "time, error, area, " + columns +
                    " FROM " + timeseries +
                    (starttime == null ? "" : " WHERE time >= '" + starttime + "'") +
                    (endtime   == null ? "" : " AND time < '" + endtime + "'") +
                    (conditions    == null ? "" : " " + conditions);
            System.out.println(sql);
            ResultSet resultSet = pgtool.query(connection, sql);

            long stime = System.currentTimeMillis();

            System.out.println("exec used time: " + (System.currentTimeMillis() - stime) + "ms");

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
                        else if(Types.DATE == type) map.put(label, resultSet.getDate(i));
                        else if(Types.TIME == type) map.put(label, resultSet.getTime(i));
                        else if(Types.TIMESTAMP == type) map.put(label, resultSet.getTimestamp(i));
                        else map.put(label, resultSet.getString(i));
                    }
                    res.add(map);
                }
            }
            connection.close();
            System.out.println("used time: " + (System.currentTimeMillis() - stime) + "ms");
        }
        else if(dbtype.toLowerCase().equals("influxdb")){
            if(ip != null && port != null) url = String.format("http://%s:%s/", ip, port);

            InfluxDBConnection influxDBConnection = new InfluxDBConnection(url, username, password, database, null);
            String sql = query != null ? query :
                    "SELECT " + columns +
                    " FROM " + timeseries +
                    (starttime == null ? "" : " WHERE time >= " + starttime) +
                    (endtime   == null ? "" : " AND time < " + endtime) +
                    (conditions    == null ? "" : conditions);
            System.out.println(sql);
            QueryResult queryResult = influxDBConnection.query(sql);

            long stime = System.currentTimeMillis();
            System.out.println("exec used time: " + (System.currentTimeMillis() - stime) + "ms");

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
            System.out.println("used time: " + (System.currentTimeMillis() - stime) + "ms");
        }
        else if(dbtype.equals("kafka")){
            System.out.println(dbtype);
            Properties props = new Properties();
            props.put("bootstrap.servers", "192.168.10.172:9093");
            props.put("group.id", "test");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
            props.put("value.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
            props.put("auto.offset.reset","earliest");

            KafkaConsumer<Long, Integer> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(database));
            ConsumerRecords<Long, Integer> records = consumer.poll(1000);
            System.out.println(records.count());
            for (ConsumerRecord<Long, Integer> record : records) {
                Map<String, Object> map = new HashMap<>();
                map.put("time", record.key());
                map.put("value", record.value());
                res.add(map);
            }
            consumer.close();
        }
        else
            return null;

        Collections.sort(res, sampleComparator);

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
