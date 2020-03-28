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

//        List<Bucket> buckets = new BucketController().buckets(url, username, password, database, timeseries, columns, starttime, endtime, conditions, query,"map", ip, port, amount, dbtype, percent, alpha);
        List<Bucket> buckets = new GBucketController().buckets(url, username, password, database, timeseries, columns, starttime, endtime, conditions, query,"map", ip, port, amount, dbtype, percent, alpha);
        List<Map<String, Object>> res = new LinkedList<Map<String, Object>>();
        long st = System.currentTimeMillis();
        System.out.println("gradsample started");
        for(Bucket bucket : buckets){
            theta = bucket.getDataPoints().size() / 4;
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
                double weight = (double)data.get("weight");
//                Object value = data.get(label);
//                if(value instanceof Double) weight = (Double)value;
//                else if(value instanceof Integer) weight = ((Integer) value).doubleValue();
//                else if(value instanceof Long) weight = ((Long) value).doubleValue();
//                else weight = (Double)data.get(label);

                double sim = 0.0;
                for(int j = i; j > i - theta && j >= 0; j--) {
                    double diff;
                    Object v1 = datapoints.get(j).get(label);
                    Object v2 = data.get(label);
                    if(v1 instanceof Double) diff = (Double) v1 - (Double) v2;
                    else if(v1 instanceof Integer) diff = ((Integer) v1).doubleValue() - ((Integer)v2).doubleValue();
                    else if(v1 instanceof Long) diff = ((Long) v1).doubleValue() - ((Long)v2).doubleValue();
                    else diff = (Double) v1 - (Double) v2;
                    sim = Math.max(Math.abs(diff), sim);
                }
//                if(!maxWeight.isEmpty() && i - maxWeight.peek() >= theta) maxWeight.poll();
//                if(!minWeight.isEmpty() && i - minWeight.peek() >= theta) minWeight.poll();

//                while (!maxWeight.isEmpty()){
//                    Object v1 = datapoints.get(maxWeight.peek()).get(label);
//                    double v;
//                    if(v1 instanceof Double) v = (Double) v1;
//                    else if(v1 instanceof Integer) v = ((Integer) v1).doubleValue();
//                    else if(v1 instanceof Long) v = ((Long) v1).doubleValue();
//                    else v = (Double) v1;
//                    if(weight >= v) maxWeight.poll();
//                    else break;
//                }
//                maxWeight.offer(i);
//                while (!minWeight.isEmpty()){
//                    Object v1 = datapoints.get(minWeight.peek()).get(label);
//                    double v;
//                    if(v1 instanceof Double) v = (Double) v1;
//                    else if(v1 instanceof Integer) v = ((Integer) v1).doubleValue();
//                    else if(v1 instanceof Long) v = ((Long) v1).doubleValue();
//                    else v = (Double) v1;
//                    if(weight >= v) minWeight.poll();
//                    else break;
//                }
//                minWeight.offer(i);
//
//                Object value1 = datapoints.get(maxWeight.peek()).get(label);
//                double maxValue, minValue;
//                if(value1 instanceof Double) maxValue = (Double)value1;
//                else if(value1 instanceof Integer) maxValue = ((Integer) value1).doubleValue();
//                else if(value1 instanceof Long) maxValue = ((Long) value1).doubleValue();
//                else maxValue = (Double)value1;
//
//                Object value2 = datapoints.get(minWeight.peek()).get(label);
//                if(value2 instanceof Double) minValue = (Double)value2;
//                else if(value2 instanceof Integer) minValue = ((Integer) value2).doubleValue();
//                else if(value2 instanceof Long) minValue = ((Long) value2).doubleValue();
//                else minValue = (Double)value2;
//                sim = Math.max(maxValue - weight, weight - minValue);
                H.offer(new BucketDataPoint(data, i, sim*weight));
            }
            for(int i = 0; i < k; i++){
                BucketDataPoint c = H.poll();
                if(c == null) break;
                while (c.getIter() != candi.size()){
                    if(!ids.contains(c.getId())){
                        double sim = 0;
                        for(int j = i; j > i - theta && j >= 0 && !candi.contains(datapoints.get(j)); j--) {
                            double diff;
                            Object v1 = c.getData().get(label);
                            Object v2 = datapoints.get(j).get(label);
                            if(v1 instanceof Double) diff = (Double) v1 - (Double) v2;
                            else if(v1 instanceof Integer) diff = ((Integer) v1).doubleValue() - ((Integer)v2).doubleValue();
                            else if(v1 instanceof Long) diff = ((Long) v1).doubleValue() - ((Long)v2).doubleValue();
                            else diff = (Double) v1 - (Double) v2;
                            sim = Math.max(Math.abs(diff), sim);
                        }
                        c.setSim(sim * (double)c.getData().get("weight"));
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
