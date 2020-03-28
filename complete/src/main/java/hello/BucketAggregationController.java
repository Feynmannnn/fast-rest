package hello;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@RestController
public class BucketAggregationController {
    @RequestMapping("/bucketaggregation")
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
            @RequestParam(value="dbtype", defaultValue = "iotdb") String dbtype,
            @RequestParam(value="percent", defaultValue = "1") Double percent,
            @RequestParam(value="alpha", defaultValue = "1") Double alpha
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

        String iotdblabel = database + "." + timeseries + "." +columns;
        String label = dbtype.equals("iotdb") ? iotdblabel : columns;
        String timelabel = "time";

//        List<Bucket> buckets = new BucketController().buckets(url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, "map", ip, port, amount*4, dbtype, percent, alpha);
        List<Bucket> buckets = new GBucketController().buckets(url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, "map", ip, port, amount*4, dbtype, percent, alpha);
        List<Map<String, Object>> res = new LinkedList<>();
        long st = System.currentTimeMillis();
        System.out.println("bucketaggregation started");

        for(Bucket bucket : buckets){
            List<Map<String, Object>> datapoints = bucket.getDataPoints();
            if(datapoints.size() == 0) continue;
            double valueSum = 0;

            for(int i = 0; i < datapoints.size(); i++){
                Map<String, Object> candi = datapoints.get(i);
                Object value = candi.get(label);
                if(value instanceof Double) valueSum += (double)value;
                else if(value instanceof Integer) valueSum += (double)((Integer)value);
                else if(value instanceof Long) valueSum += (double)((Long)value);
                else valueSum += 0;
            }

            Map<String, Object> obj = new HashMap<>();
            obj.put(timelabel, datapoints.get(datapoints.size()-1).get(timelabel));
            obj.put(label, valueSum / datapoints.size());
            res.add(obj);
        }
        System.out.println("bucketaggregation used time: " + (System.currentTimeMillis() - st) + "ms");
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
