package hello;import java.util.*;

import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;
import java.util.Date;

import org.apache.iotdb.jdbc.IoTDBSQLException;

import javax.script.*;

@RestController
public class SimpleM4Controller {
    @RequestMapping("/simplem4")
    public List<Map<String, Object>> dataPoints(
            @RequestParam(value="url", defaultValue = "jdbc:iotdb://127.0.0.1:6667/") String url,
            @RequestParam(value="username", defaultValue = "root") String username,
            @RequestParam(value="password", defaultValue = "root") String password,
            @RequestParam(value="database") String database,
            @RequestParam(value="timeseries") String timeseries,
            @RequestParam(value="columns") String columns,
            @RequestParam(value="starttime", required = false) String starttime,
            @RequestParam(value="endtime", required = false) String endtime,
            @RequestParam(value="conditions", required = false) String conditions,
            @RequestParam(value="query", required = false) String query,
            @RequestParam(value="format", defaultValue = "map") String format,
            @RequestParam(value="ip", required = false) String ip,
            @RequestParam(value="port", required = false) String port,
            @RequestParam(value="amount", required = false) Integer amount,
            @RequestParam(value="dbtype", defaultValue = "iotdb") String dbtype
    ) throws Exception {
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

        List<Map<String, Object>> linkedDataPoints = new DataPointController().dataPoints(
                url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, "map", ip, port, dbtype);
        if(linkedDataPoints.size() < 2) return null;

        List<Map<String, Object>> dataPoints = new ArrayList<>(linkedDataPoints);

        Long firstTimestamp = (Timestamp.valueOf(dataPoints.get(0).get("time").toString().replace("T", " ").replace("Z", ""))).getTime();
        Long lastTimestamp = (Timestamp.valueOf(dataPoints.get(dataPoints.size()-1).get("time").toString().replace("T", " ").replace("Z", ""))).getTime();
        Long timestampRange = lastTimestamp - firstTimestamp;
        Long timeinteval = timestampRange / amount * 4;

        String iotdbLabel = database + "." + timeseries + "." +columns;
        String label = dbtype.equals("iotdb") ? iotdbLabel : columns;
        String timelabel = "time";

        for(Map<String, Object> dataPoint : dataPoints) dataPoint.put(timelabel, dataPoint.get(timelabel).toString().replace("T", " "));
        List<Bucket> buckets = new LinkedList<>();

        int p = 0, q = 0;
        int n = dataPoints.size();
        lastTimestamp = firstTimestamp + timeinteval;
        while (p < n){
            Long dataTimestamp = (Timestamp.valueOf(dataPoints.get(p).get("time").toString().replace("T", " ").replace("Z", ""))).getTime();
            if(dataTimestamp > lastTimestamp){
                buckets.add(new Bucket(dataPoints.subList(q, p)));
                q = p;
                lastTimestamp += timeinteval;
            }
            p++;
        }
        buckets.add(new Bucket(dataPoints.subList(q, p)));

        String iotdblabel = database + "." + timeseries + "." +columns;
        label = dbtype.equals("iotdb") ? iotdblabel : columns;

        List<Map<String, Object>> res = new LinkedList<>();
        long st = System.currentTimeMillis();
        System.out.println("m4sample started");

        for(Bucket bucket : buckets){
            List<Map<String, Object>> datapoints = bucket.getDataPoints();
            if(datapoints.size() <= 4){
                res.addAll(datapoints);
                continue;
            }

            if(datapoints.size() < 4) {res.addAll(datapoints); continue;}
            res.add(datapoints.get(0));
            Map<String, Object> maxi = datapoints.get(0);
            Map<String, Object> mini = datapoints.get(0);
            for(int i = 1; i < datapoints.size()-1; i++){
                Map<String, Object> candi = datapoints.get(i);
                Object value = candi.get(label);
                if(value instanceof Double) maxi = (Double) value >= (Double)maxi.get(label) ? candi : maxi;
                else if(value instanceof Integer) maxi = (Integer) value >= (Integer)maxi.get(label) ? candi : maxi;
                else if(value instanceof Long) maxi = (Long) value >= (Long)maxi.get(label) ? candi : maxi;
                else maxi = (Double) value >= (Double)maxi.get(label) ? candi : maxi;

                if(value instanceof Double) mini = (Double) value <= (Double)mini.get(label) ? candi : mini;
                else if(value instanceof Integer) mini = (Integer) value <= (Integer)mini.get(label) ? candi : mini;
                else if(value instanceof Long) mini = (Long) value <= (Long)mini.get(label) ? candi : mini;
                else mini = (Double) value <= (Double)mini.get(label) ? candi : mini;
            }
            res.add(maxi);
            res.add(mini);
            res.add(datapoints.get(datapoints.size()-1));
        }
        System.out.println("m4sample used time: " + (System.currentTimeMillis() - st) + "ms");
        if(format.equals("map")) return res;

        timelabel = "time";

        List<Map<String, Object>> result = new LinkedList<>();
        for(Map<String, Object> map : res){
            Object time = map.get(timelabel);
            for(Map.Entry<String, Object> entry : map.entrySet()){
                String mapKey = entry.getKey();
                if(mapKey.equals(timelabel)) continue;
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
