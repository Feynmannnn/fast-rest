package hello;

import java.util.*;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;

@RestController
public class BucketSampleController {

    public static Comparator<BucketDataPoint> bucketComparator = new Comparator<BucketDataPoint>(){
        @Override
        public int compare(BucketDataPoint bucketDataPoint1, BucketDataPoint bucketDataPoint2){
            return (int)-Math.round(bucketDataPoint1.getSim() - bucketDataPoint2.getSim());
        }
    };

    @RequestMapping("/bucketsample")
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
            @RequestParam(value="theta", defaultValue = "50") Integer theta,
            @RequestParam(value="k", defaultValue = "4") Integer k,
            @RequestParam(value="format", defaultValue = "map") String format,
            @RequestParam(value="ip", required = false) String ip,
            @RequestParam(value="port", required = false) String port,
            @RequestParam(value="amount", required = false) Integer amount,
            @RequestParam(value="dbtype", defaultValue = "iotdb") String dbtype,
            @RequestParam(value="percent", defaultValue = "99995") Long percent,
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

        List<Bucket> buckets = new BucketController().buckets(url, username, password, database, timeseries, columns, starttime, endtime, conditions, query,"map", ip, port, amount, dbtype, percent, alpha);
        if(buckets == null) return null;

        List<Map<String, Object>> res = new LinkedList<Map<String, Object>>();
        // add sepecial first data point
        res.add(buckets.get(0).getDataPoints().get(0));
        long st = System.currentTimeMillis();
        System.out.println("bucketsample started");
        for(Bucket bucket : buckets){
            List<Map<String, Object>> datapoints = bucket.getDataPoints();
            if(datapoints.size() <= k){
                res.addAll(datapoints);
                continue;
            }

            Set<Map<String, Object>> candi = new HashSet<>();
            Set<Integer> ids = new HashSet<>();
            Queue<BucketDataPoint> H = new PriorityQueue<>(datapoints.size(), bucketComparator);
            for(int i = 0; i < datapoints.size(); i++){
                Map<String, Object> data = datapoints.get(i);
                double sim = (Double) data.get("weight") + 0.0;
                H.offer(new BucketDataPoint(data, i, sim));
            }
            for(int i = 0; i < k; i++){
                BucketDataPoint c = H.poll();
                if(c == null) break;
                while (c.getIter() != candi.size()){
                    if(!ids.contains(c.getId())){
                        c.setIter(candi.size());
                        H.offer(c);
                    }
                    c = H.poll();
                    if(c == null) break;
                }
                if(c == null) break;
                candi.add(c.getData());
                int id = c.getId();
                for(int j = id; j > id - theta && j >= 0; j--){
                    ids.add(j);
                }
            }
            res.addAll(candi);
        }
        // add sepecial last data point
        List<Map<String, Object>> lastBucket = buckets.get(buckets.size()-1).getDataPoints();
        if(lastBucket.size() > 0) res.add(lastBucket.get(lastBucket.size()-1));

        System.out.println("bucketsample used time: " + (System.currentTimeMillis() - st) + "ms");
        if(format.equals("map")) return res;

        String timelabel = "time";
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
