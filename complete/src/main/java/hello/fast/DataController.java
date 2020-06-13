package hello.fast;

import hello.fast.source.InfluxDBConnection;
import hello.fast.source.IoTDBConnection;
import hello.fast.source.PGConnection;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.*;
import java.sql.*;

import org.apache.iotdb.rpc.IoTDBRPCException;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.thrift.TException;
import org.influxdb.dto.QueryResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;

/**
* 数据控制器，根据参数查询原始数据
*/
@RestController
public class DataController {

    private static Comparator<Map<String, Object>> sampleComparator = new Comparator<Map<String, Object>>(){
        @Override
        public int compare(Map<String, Object> sampleDataPoint1, Map<String, Object> sampleDataPoint2){
            long t1 = (long)sampleDataPoint1.get("timestamp");
            long t2 = (long)sampleDataPoint2.get("timestamp");
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
            @RequestParam(value="timeColumn", defaultValue = "time") String timecolumn,
            @RequestParam(value="startTime", required = false) String starttime,
            @RequestParam(value="endTime", required = false) String endtime,
            @RequestParam(value="conditions", required = false) String conditions,
            @RequestParam(value="query", required = false) String query,
            @RequestParam(value="format", defaultValue = "map") String format,
            @RequestParam(value="ip", required = false) String ip,
            @RequestParam(value="port", required = false) String port,
            @RequestParam(value="dbtype", defaultValue = "iotdb") String dbtype
    ) throws SQLException, IoTDBSessionException, TException, IoTDBRPCException {
        // trim the '"' of the parameters
        url = url.replace("\"", "");
        username = username.replace("\"", "");
        password = password.replace("\"", "");
        database = database.replace("\"", "");
        timeseries = timeseries.replace("\"", "");
        columns = columns.replace("\"", "");
        timecolumn = timecolumn.replace("\"", "");
        starttime = starttime == null ? null : starttime.replace("\"", "");
        endtime = endtime == null ? null :endtime.replace("\"", "");
        conditions = conditions == null ? null : conditions.replace("\"", "");
        format = format.replace("\"", "");
        dbtype = dbtype.replace("\"", "");
        ip = ip == null ? null : ip.replace("\"", "");
        port = port == null ? null : port.replace("\"", "");
        query = query == null ? null : query.replace("\"", "");

        return _dataPoints(url, username, password, database, timeseries, columns, timecolumn, starttime, endtime, conditions, query, format, ip, port, dbtype);
    }

    public static List<Map<String, Object>> _dataPoints(
            String url,
            String username,
            String password,
            String database,
            String timeseries,
            String columns,
            String timecolumn,
            String starttime,
            String endtime,
            String conditions,
            String query,
            String format,
            String ip,
            String port,
            String dbtype
    ) throws SQLException, IoTDBSessionException, TException, IoTDBRPCException {

        List<Map<String, Object>> res = new ArrayList<>();
        System.out.println(dbtype);
        long t1 = System.currentTimeMillis();

        if(dbtype.toLowerCase().equals("iotdb")){
            if(ip != null && port != null) url = String.format("jdbc:iotdb://%s:%s/", ip, port);

            org.apache.iotdb.jdbc.IoTDBConnection connection = (org.apache.iotdb.jdbc.IoTDBConnection) IoTDBConnection.getConnection(url, username, password);
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
                        else if(Types.TIMESTAMP == type) {
                            long timestamp = resultSet.getLong(i);
                            if(timestamp < 2000000000000L) timestamp *= 1000000L;
                            else if(timestamp < 2000000000000000L) timestamp *= 1000L;
                            map.put("timestamp", timestamp);
                            map.put("time", new Timestamp(timestamp/1000000L).toString() + String.format("%06d", timestamp%1000000L));
                        }
                        else map.put(label, resultSet.getString(i));
                    }
                    res.add(map);
                }
            }
            System.out.println(res.size());
            statement.close();
            connection.close();
        }
        else if(dbtype.toLowerCase().equals("session")) {
            if(ip == null) ip = "127.0.0.1";
            if(port == null) port = "6667";
            Session session = new Session(ip, port, username, password);
            session.open();

            session.setStorageGroup(database);

            String sql = query != null ? query :
                    "SELECT " + columns +
                    " FROM " + database + "." + timeseries +
                    (starttime == null ? "" : " WHERE time >= " + starttime) +
                    (endtime   == null ? "" : " AND time < " + endtime) +
                    (conditions == null ? "" : conditions);
            System.out.println(sql);

            SessionDataSet dataSet;
            dataSet = session.executeQueryStatement(sql);
            while (dataSet.hasNext()) {
                RowRecord data = dataSet.next();
                Map<String, Object> map = new HashMap<>();
                long timestamp = data.getTimestamp();
                if(timestamp < 2000000000000L) timestamp *= 1000000L;
                else if(timestamp < 2000000000000000L) timestamp *= 1000L;
                map.put("timestamp", timestamp);
                map.put("time", new Timestamp(timestamp/1000000L).toString() + String.format("%06d", timestamp%1000000L));
                List<Field> fields = data.getFields();
                map.put(columns, fields.get(0).getDoubleV());
                res.add(map);
            }
            System.out.println(res.size());
            dataSet.closeOperationHandle();
            session.close();

        }
        else if(dbtype.toLowerCase().equals("postgresql") || dbtype.toLowerCase().equals("timescaledb")){
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
            sql = sql.replace("time", timecolumn);
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
                        else if(Types.TIMESTAMP == type) {
                            map.put("time", resultSet.getString(i));
                            map.put("timestamp", resultSet.getTimestamp(i).getTime() * 1000000L + LocalDateTime.parse(resultSet.getString(i).replace(" ", "T").replace("+08", "")).getNano() % 1000000L);
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
                    (starttime == null ? "" : (" WHERE time >= '" + starttime + "'")) +
                    (endtime   == null ? "" : (" AND time < '" + endtime + "' ")) +
                    (conditions == null ? "" : conditions) + ";";
            System.out.println(sql);
            QueryResult queryResult = influxDBConnection.query(sql);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            for(QueryResult.Result result : queryResult.getResults()){
                for(QueryResult.Series series : result.getSeries()){
                    List<String> cols = series.getColumns();
                    for(List<Object> objects :series.getValues()){
                        Map<String, Object> map = new HashMap<>();
                        for(int i = 0; i < objects.size(); i++){
                            if(cols.get(i).equals("time")) {
                                String time = objects.get(i).toString().replace("T", " ").replace("Z", "");
                                try {
                                    long timestamp = sdf.parse(time.substring(0,19)).getTime() * 1000000L + LocalDateTime.parse(time.replace(" ", "T").replace("+08", "")).getNano();
                                    map.put("timestamp", timestamp);
                                    time = sdf.format(new Date(timestamp / 1000000)) + "." + String.format("%09d", timestamp % 1000000000L);
                                    map.put("time", time);
                                } catch (ParseException e) {
                                    e.printStackTrace();
                                }
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

            // Kafka数据模型定义尚不完善，目前仅测试版本
            System.out.println(url);
            System.out.println(timeseries);
            System.out.println(columns);

            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

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
            ConsumerRecords<Long, Double> records = consumer.poll(Duration.ofMillis(1000));
            System.out.println(records.count());
            for (ConsumerRecord<Long, Double> record : records) {
                Map<String, Object> map = new HashMap<>();
                long timestamp = record.key();
                map.put("timestamp", record.key());
                map.put("time", sdf.format(new Date(timestamp / 1000000)) + "." + String.format("%09d", timestamp % 1000000000L));
                map.put(columns.toLowerCase(), record.value());
                res.add(map);
            }
            consumer.close();
        }
        else return res;

        // 部分数据源存在查询结果时间乱序问题
        res.sort(sampleComparator);

        System.out.println("DataController: " + (System.currentTimeMillis() - t1) + "ms");

        if(format.equals("map")) return res;
        List<Map<String, Object>> result = new ArrayList<>();
        for(Map<String, Object> map : res){
            for(Map.Entry<String, Object> entry : map.entrySet()){
                String mapKey = entry.getKey();
                if(mapKey.equals("time")) continue;
                Map<String, Object> m = new HashMap<>();
                m.put("time", map.get("time"));
                m.put("label", mapKey);
                m.put("value", entry.getValue());
                result.add(m);
            }
        }
        return result;
    }

    public static long _dataPointsCount(
            String url,
            String username,
            String password,
            String database,
            String timeseries,
            String columns,
            String timecolumn,
            String starttime,
            String endtime,
            String conditions,
            String query,
            String format,
            String ip,
            String port,
            String dbtype
    ) throws SQLException {

        Long res = 0L;
        System.out.println(dbtype);

        if(dbtype.toLowerCase().equals("iotdb") || dbtype.toLowerCase().equals("session")){
            if(ip != null && port != null) url = String.format("jdbc:iotdb://%s:%s/", ip, port);

            Connection connection = IoTDBConnection.getConnection(url, username, password);
            if (connection == null) {
                System.out.println("get connection defeat");
                return 0;
            }
            Statement statement = connection.createStatement();
            String sql = query != null ? query :
                    "SELECT count(" + columns +
                    ") FROM " + database + "." + timeseries +
                    (starttime == null ? "" : " WHERE time >= " + starttime) +
                    (endtime   == null ? "" : " AND time < " + endtime) +
                    (conditions == null ? "" : conditions);
            System.out.println(sql);
            ResultSet resultSet = statement.executeQuery(sql);

            if (resultSet != null) {
                while (resultSet.next()) {
                    res = Long.valueOf(resultSet.getString(2));
                    System.out.println(res);
                }
            }
            statement.close();
            connection.close();
        }
        else if(dbtype.toLowerCase().equals("postgresql") || dbtype.toLowerCase().equals("timescaledb")){
            if(ip != null && port != null) url = String.format("jdbc:postgresql://%s:%s/", ip, port);

            PGConnection pgtool = new PGConnection(url+database, username, password);
            Connection connection = pgtool.getConn();
            if (connection == null) {
                System.out.println("get connection defeat");
                return res;
            }
            String sql = query != null ? query :
                    "SELECT " + "count(" + columns +
                    ") FROM " + timeseries +
                    (starttime == null ? "" : " WHERE time >= '" + starttime + "'") +
                    (endtime   == null ? "" : " AND time < '" + endtime + "'") +
                    (conditions == null ? "" : " " + conditions);
            System.out.println(sql);
            sql = sql.replace("time", timecolumn);
            ResultSet resultSet = pgtool.query(connection, sql);

            if (resultSet != null) {
                while (resultSet.next()) {
                    res = Long.valueOf(resultSet.getString(1));
                }
            }
            connection.close();
        }
        else if(dbtype.toLowerCase().equals("influxdb")){
            if(ip != null && port != null) url = String.format("http://%s:%s/", ip, port);

            InfluxDBConnection influxDBConnection = new InfluxDBConnection(url, username, password, database, null);
            String sql = query != null ? query :
                    "SELECT count(" + columns +
                    ") FROM " + timeseries +
                    (starttime == null ? "" : " WHERE time >= '" + starttime) +
                    (endtime   == null ? "" : "' AND time < '" + endtime) + "' " +
                    (conditions == null ? "" : conditions) + ";";
            System.out.println(sql);
            QueryResult queryResult = influxDBConnection.query(sql);

            for(QueryResult.Result result : queryResult.getResults()){
                for(QueryResult.Series series : result.getSeries()){
                    for(List<Object> objects :series.getValues()){
                            res = Double.valueOf(objects.get(1).toString()).longValue();
                    }
                }
            }
            influxDBConnection.close();
        }

        return res;
    }
}
