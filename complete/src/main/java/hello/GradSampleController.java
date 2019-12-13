package hello;

import java.util.*;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;

@RestController
public class GradSampleController {

    public static Comparator<BucketDataPoint> bucketComparator = new Comparator<BucketDataPoint>(){
        @Override
        public int compare(BucketDataPoint bucketDataPoint1, BucketDataPoint bucketDataPoint2){
            return (int)-Math.round(bucketDataPoint1.getSim() - bucketDataPoint2.getSim());
        }
    };

    @RequestMapping("/gradsample")
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
            @RequestParam(value="dbtype", defaultValue = "iotdb") String dbtype
    ) throws SQLException {

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

        List<Bucket> buckets = new BucketController().buckets(url, username, password, database, timeseries, columns, starttime, endtime, conditions, query,"map", ip, port, amount, dbtype);
        List<Map<String, Object>> res = new LinkedList<Map<String, Object>>();
        long st = System.currentTimeMillis();
        System.out.println("gradsample started");
        for(Bucket bucket : buckets){
            List<Map<String, Object>> datapoints = bucket.getDataPoints();
            if(datapoints.size() <= k){
                res.addAll(datapoints);
                continue;
            }

            Set<Map<String, Object>> candi = new HashSet<>();
            Set<Integer> ids = new HashSet<>();
            Queue<BucketDataPoint> H = new PriorityQueue<>(datapoints.size(), bucketComparator);
            Queue<Integer> maxWeight = new LinkedList<>();
            Queue<Integer> minWeight = new LinkedList<>();
            for(int i = 0; i < datapoints.size(); i++){
                Map<String, Object> data = datapoints.get(i);
                double weight = (Double)data.get(label);
                double sim = 0.0;
                for(int j = i; j > i - theta && j >= 0; j--) sim = Math.max(Math.abs((Double)data.get(label) - (Double)datapoints.get(j).get(label)), sim);
                if(!maxWeight.isEmpty() && i - maxWeight.peek() >= theta) maxWeight.poll();
                if(!minWeight.isEmpty() && i - minWeight.peek() >= theta) minWeight.poll();
                while (!maxWeight.isEmpty() && weight >= (Double)datapoints.get(maxWeight.peek()).get(label)){
                    maxWeight.poll();
                }
                maxWeight.offer(i);
                while (!minWeight.isEmpty() && weight >= (Double)datapoints.get(minWeight.peek()).get(label)){
                    minWeight.poll();
                }
                minWeight.offer(i);
                sim = Math.max((Double)datapoints.get(maxWeight.peek()).get(label) - weight, weight - (Double)datapoints.get(minWeight.peek()).get(label));
                H.offer(new BucketDataPoint(data, i, sim));
            }
            for(int i = 0; i < k; i++){
                BucketDataPoint c = H.poll();
                if(c == null) break;
                while (c.getIter() != candi.size()){
                    if(!ids.contains(c.getId())){
                        double sim = 0;
                        for(int j = i; j > i - theta && j >= 0 && !candi.contains(datapoints.get(j)); j--) sim = Math.max(Math.abs((Double)c.getData().get(label) - (Double)datapoints.get(j).get(label)), sim);
                        c.setSim(sim);
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
        System.out.println("gradsample used time: " + (System.currentTimeMillis() - st) + "ms");
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
