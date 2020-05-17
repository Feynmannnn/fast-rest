package hello.refactor;

import com.alibaba.fastjson.JSONObject;
import hello.refactor.meta.TimeSeries;
import hello.refactor.meta.TimeSeriesController;
import org.springframework.util.DigestUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@RestController
public class QueryController {

    private static final String salt = "&%12345***&&%%$$#@1";

    @RequestMapping("/query")
    public List<Map<String, Object>> publish(
            @RequestParam(value="url", defaultValue = "jdbc:iotdb://127.0.0.1:6667/") String url,
            @RequestParam(value="database") String database,
            @RequestParam(value="timeseries") String timeseries,
            @RequestParam(value="columns") String columns,
            @RequestParam(value="starttime", required = false) String starttime,
            @RequestParam(value="endtime", required = false) String endtime,
            @RequestParam(value="amount", required = false) Long amount,
            @RequestParam(value="ip", required = false) String ip,
            @RequestParam(value="port", required = false) String port,
            @RequestParam(value="dbtype", defaultValue = "iotdb") String dbtype,
            @RequestParam(value="format", defaultValue = "map") String format
    ) throws SQLException {
        url = url.replace("\"", "");
        database = database.replace("\"", "");
        timeseries = timeseries.replace("\"", "");
        columns = columns.replace("\"", "");
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

        Long t = System.currentTimeMillis();

        String config = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader("fast.config"));
            String str = "";
            StringBuilder sb = new StringBuilder();
            while ((str = br.readLine()) != null) {
                str=new String(str.getBytes(),"UTF-8");//解决中文乱码问题
                sb.append(str);
            }
            config = sb.toString();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        JSONObject jsonObject = JSONObject.parseObject(config);
        String autovisURL = jsonObject.getString("autovisURL");
        String innerUrl = jsonObject.getString("innerURL");
        String innerUserName = jsonObject.getString("innerusername");
        String innerPassword = jsonObject.getString("innerpassword");

        // iotdb is . tsdb is _
        String[] tables = subTables(url, innerUrl, innerUserName, innerPassword, database, timeseries, columns);

        List<Map<String, Object>> res = null;
        for (String tableName : tables) {
            System.out.println(tableName);
            res = DataController._dataPoints(
                    innerUrl, innerUserName, innerPassword, database.replace(".", "_"), tableName, columns, starttime, endtime, null, null, "map", null, null, "pg");
            System.out.println(tableName);
            if (res.size() >= amount) {
                break;
            }
        }

        System.out.println("publish used time:" + (System.currentTimeMillis() - t));
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
            @RequestParam(value="starttime", required = false) String starttime,
            @RequestParam(value="endtime", required = false) String endtime,
            @RequestParam(value="percent", required = false) Double percent,
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

        Long t = System.currentTimeMillis();

        String config = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader("fast.config"));
            String str = "";
            StringBuilder sb = new StringBuilder();
            while ((str = br.readLine()) != null) {
                str=new String(str.getBytes(),"UTF-8");//解决中文乱码问题
                sb.append(str);
            }
            config = sb.toString();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        JSONObject jsonObject = JSONObject.parseObject(config);
        String autovisURL = jsonObject.getString("autovisURL");
        String innerUrl = jsonObject.getString("innerURL");
        String innerUserName = jsonObject.getString("innerusername");
        String innerPassword = jsonObject.getString("innerpassword");

        // iotdb is . tsdb is _
        String[] tables = subTables(url, innerUrl, innerUserName, innerPassword, database, timeseries, columns);

        List<Map<String, Object>> res = null;
        for(String tableName : tables){
            System.out.println(tableName);
            res = DataController._dataPoints(
                    innerUrl, innerUserName, innerPassword, database.replace(".", "_"), tableName, columns + ", error, area", starttime, endtime, null, null, "map", null, null, "pg");
            System.out.println(tableName);

            double error= 0.0;
            double area = 0.0;

            for(int i = 0; i < res.size(); i++){
                error += (double)res.get(i).get("error");
                area += (double)res.get(i).get("area");
            }

            System.out.println(error / area);
            if((error / area) <= percent) break;
        }

        System.out.println("publish used time:" + (System.currentTimeMillis() - t));
        return res;
    }

    static String[] subTables(String url, String innerurl, String username, String password, String database, String timeseries, String columns) throws SQLException {
        try{
            List<TimeSeries> timeSeries = new TimeSeriesController().timeSeries(innerurl, username, password, database.replace(".", "_"), null, null, "pg");
            int n = timeSeries.size();
            System.out.println(n);
            String[] res = new String[n];
            System.out.println(url);
            System.out.println(database);
            System.out.println(timeseries);
            System.out.println(columns);
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
