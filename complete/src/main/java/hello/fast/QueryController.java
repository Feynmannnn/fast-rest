package hello.fast;

import com.alibaba.fastjson.JSONObject;
import hello.fast.meta.TimeSeries;
import hello.fast.meta.TimeSeriesController;
import org.springframework.util.DigestUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.*;

@RestController
public class QueryController {

    private static final String salt = "&%12345***&&%%$$#@1";

    @RequestMapping("/query")
    public List<Map<String, Object>> publish(
            @RequestParam(value="url", defaultValue = "jdbc:iotdb://127.0.0.1:6667/") String url,
            @RequestParam(value="username", defaultValue = "root") String username,
            @RequestParam(value="password", defaultValue = "root") String password,
            @RequestParam(value="database") String database,
            @RequestParam(value="timeseries") String timeseries,
            @RequestParam(value="columns") String columns,
            @RequestParam(value="timecolumn", defaultValue = "time") String timecolumn,
            @RequestParam(value="starttime", required = false) String starttime,
            @RequestParam(value="endtime", required = false) String endtime,
            @RequestParam(value="amount", required = false) Long amount,
            @RequestParam(value="ip", required = false) String ip,
            @RequestParam(value="port", required = false) String port,
            @RequestParam(value="dbtype", defaultValue = "iotdb") String dbtype,
            @RequestParam(value="format", defaultValue = "map") String format
    ) throws SQLException {
        url = url.replace("\"", "");
        username = username.replace("\"", "");
        password = password.replace("\"", "");
        database = database.replace("\"", "");
        timeseries = timeseries.replace("\"", "");
        columns = columns.replace("\"", "");
        timecolumn = timecolumn.replace("\"", "");
        starttime = starttime == null ? null : starttime.replace("\"", "");
        endtime = endtime == null ? null : endtime.replace("\"", "");
        format = format.replace("\"", "");
        ip = ip == null ? null : ip.replace("\"", "");
        port = port == null ? null : port.replace("\"", "");
        dbtype = dbtype.replace("\"", "");

        if(dbtype.toLowerCase().equals("iotdb")) {
            if (ip != null && port != null) url = String.format("jdbc:iotdb://%s:%s/", ip, port);
        }
        else if(dbtype.toLowerCase().equals("pg")) {
            if (ip != null && port != null) url = String.format("jdbc:postgresql://%s:%s/", ip, port);
        }
        else if(dbtype.toLowerCase().equals("influxdb")) {
            if (ip != null && port != null) url = String.format("http://%s:%s/", ip, port);
        }
        else{
            if (ip != null && port != null) url = String.format("jdbc:iotdb://%s:%s/", ip, port);
        }

        String config = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader("fast.config"));
            String str = "";
            StringBuilder sb = new StringBuilder();
            while ((str = br.readLine()) != null) {
                str=new String(str.getBytes(), StandardCharsets.UTF_8);//解决中文乱码问题
                sb.append(str);
            }
            config = sb.toString();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        JSONObject jsonObject = JSONObject.parseObject(config);
        String innerUrl = jsonObject.getString("innerURL");
        String innerUserName = jsonObject.getString("innerusername");
        String innerPassword = jsonObject.getString("innerpassword");

        // iotdb is . tsdb is _
        String[] tables = subTables(url, innerUrl, innerUserName, innerPassword, database, timeseries, columns);
        // 是否已经找到对应层级
        boolean hit = false;

        List<Map<String, Object>> res = null;
        for (String tableName : tables) {
            System.out.println(tableName);
            if(!hit){
                res = new ArrayList<>(DataController._dataPoints(
                    innerUrl, innerUserName, innerPassword, database.replace(".", "_"), tableName, columns + ", weight, error, area", "time", starttime, endtime, null, null, "map", null, null, "pg"));
            }
            else{
                res.addAll(DataController._dataPoints(
                    innerUrl, innerUserName, innerPassword, database.replace(".", "_"), tableName, columns, "time", starttime, endtime, null, null, "map", null, null, "pg"));
            }
            System.out.println("res.size()" + res.size());
            if (res.size() >= amount) {
                hit = true;
                // 找到对应层级后，以最新数据点为起始时间继续向下查询
                starttime = res.get(res.size() - 1).get("time").toString();
            }
        }

        if(!hit) res = DataController._dataPoints(url, username, password, database, timeseries, columns, timecolumn, starttime, endtime, null, null, format, ip, port, dbtype);

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

    @RequestMapping("/errorquery")
    public List<Map<String, Object>> errorpublish(
            @RequestParam(value="url", defaultValue = "jdbc:iotdb://127.0.0.1:6667/") String url,
            @RequestParam(value="username", defaultValue = "root") String username,
            @RequestParam(value="password", defaultValue = "root") String password,
            @RequestParam(value="database") String database,
            @RequestParam(value="timeseries") String timeseries,
            @RequestParam(value="columns") String columns,
            @RequestParam(value="timecolumn", defaultValue = "time") String timecolumn,
            @RequestParam(value="starttime", required = false) String starttime,
            @RequestParam(value="endtime", required = false) String endtime,
            @RequestParam(value="error", required = false) Double errorPercent,
            @RequestParam(value="ip", required = false) String ip,
            @RequestParam(value="port", required = false) String port,
            @RequestParam(value="dbtype", defaultValue = "iotdb") String dbtype,
            @RequestParam(value="format", defaultValue = "map") String format
    ) throws SQLException {
        url = url.replace("\"", "");
        username = username.replace("\"", "");
        password = password.replace("\"", "");
        database = database.replace("\"", "");
        timeseries = timeseries.replace("\"", "");
        columns = columns.replace("\"", "");
        timecolumn = timecolumn.replace("\"", "");
        starttime = starttime == null ? null : starttime.replace("\"", "");
        endtime = endtime == null ? null : endtime.replace("\"", "");
        format = format.replace("\"", "");
        ip = ip == null ? null : ip.replace("\"", "");
        port = port == null ? null : port.replace("\"", "");
        dbtype = dbtype.replace("\"", "");

        if(dbtype.toLowerCase().equals("iotdb")) {
            if (ip != null && port != null) url = String.format("jdbc:iotdb://%s:%s/", ip, port);
        }
        else if(dbtype.toLowerCase().equals("pg")) {
            if (ip != null && port != null) url = String.format("jdbc:postgresql://%s:%s/", ip, port);
        }
        else if(dbtype.toLowerCase().equals("influxdb")) {
            if (ip != null && port != null) url = String.format("http://%s:%s/", ip, port);
        }
        else{
            if (ip != null && port != null) url = String.format("jdbc:iotdb://%s:%s/", ip, port);
        }

        String config = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader("fast.config"));
            String str = "";
            StringBuilder sb = new StringBuilder();
            while ((str = br.readLine()) != null) {
                str=new String(str.getBytes(), StandardCharsets.UTF_8);//解决中文乱码问题
                sb.append(str);
            }
            config = sb.toString();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        JSONObject jsonObject = JSONObject.parseObject(config);
        String innerUrl = jsonObject.getString("innerURL");
        String innerUserName = jsonObject.getString("innerusername");
        String innerPassword = jsonObject.getString("innerpassword");

        // iotdb is . tsdb is _
        String[] tables = subTables(url, innerUrl, innerUserName, innerPassword, database, timeseries, columns);
        boolean hit = false;

        List<Map<String, Object>> res = null;
        for(String tableName : tables){
            System.out.println(tableName);
            if(!hit){
                res = new ArrayList<>(DataController._dataPoints(
                    innerUrl, innerUserName, innerPassword, database.replace(".", "_"), tableName, columns + ", weight, error, area", "time", starttime, endtime, null, null, "map", null, null, "pg"));
            }
            else {
                res.addAll(DataController._dataPoints(
                    innerUrl, innerUserName, innerPassword, database.replace(".", "_"), tableName, columns + ", weight, error, area", "time", starttime, endtime, null, null, "map", null, null, "pg"));
            }
            System.out.println("res.size()" + res.size());

            double error= 0.0;
            double area = 0.0;

            for(int i = 0; i < res.size(); i++){
                error += (double)res.get(i).get("error");
                area += (double)res.get(i).get("area");
            }

            System.out.println(error / area);
            if((error / area) <= errorPercent) {
                hit = true;
            }

            if(hit) starttime = res.get(res.size() - 1).get("time").toString();
        }

        // 找不到合适的样本，查询原始数据
        if(!hit) res = DataController._dataPoints(url, username, password, database, timeseries, columns, timecolumn, starttime, endtime, null, null, format, ip, port, dbtype);

        return res;
    }

    // 数据源对应的订阅数据表
    static String[] subTables(String url, String innerurl, String username, String password, String database, String timeseries, String columns) throws SQLException {
        try{
            List<TimeSeries> timeSeries = new TimeSeriesController().timeSeries(innerurl, username, password, database.replace(".", "_"), null, null, "pg");
            int n = timeSeries.size();
            String[] res = new String[n];
            String tableName = timeseries;
            for(int i = 0; i < n; i++){
                String Identifier = String.format("%s,%s,%s,%s,%s", url, database, tableName, columns, salt);
                String newSubId = DigestUtils.md5DigestAsHex(Identifier.getBytes()).substring(0,8);
                tableName = "L" + i + "_M" + newSubId;
                res[n-1-i] = tableName;
            }
            return res;
        } catch (Exception e){
            System.out.println(e.getMessage());
            return new String[0];
        }
    }
}
