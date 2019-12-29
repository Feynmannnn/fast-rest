package hello;

import java.util.*;

import jdk.nashorn.api.scripting.ScriptObjectMirror;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;
import java.util.Date;

import org.apache.iotdb.jdbc.IoTDBSQLException;

import javax.script.*;

import static hello.GradSampleController.bucketComparator;

@RestController
public class ThesisSampleController {
    @RequestMapping("/thesissample")
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



        List<Map<String, Object>> res = new LinkedList<>();

        List<Map<String, Object>> linkedDataPoints = new DataPointController().dataPoints(
                url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, "map", ip, port, dbtype);
        List<Map<String, Object>> dataPoints = new ArrayList<>(linkedDataPoints);
        String iotdbLabel = database + "." + timeseries + "." +columns;
        String label = dbtype.equals("iotdb") ? iotdbLabel : columns;
        String timelabel = "time";

        for(Map<String, Object> dataPoint : dataPoints) dataPoint.put(timelabel, dataPoint.get(timelabel).toString().replace("T", " "));
        long stime = System.currentTimeMillis();

        List<Double> weights = new ArrayList<>();
        List<Long> timeWeights = new ArrayList<>();
        List<Double> valueWeights = new ArrayList<>();
        List<Double> valuePresum = new ArrayList<>();


        long maxTimeWeight = 0;
        long lastTimestamp = (Timestamp.valueOf(dataPoints.get(0).get(timelabel).toString().replace("T", " ").replace("Z", ""))).getTime();
        for(Map<String, Object> point : dataPoints){
            Date t = (Timestamp.valueOf(point.get(timelabel).toString().replace("T", " ").replace("Z", "")));
            Long weight = Math.abs(t.getTime() - lastTimestamp);
            timeWeights.add(weight);
            lastTimestamp = t.getTime();
            maxTimeWeight = Math.max(maxTimeWeight, weight);
        }
        for(int i = 0; i < timeWeights.size(); i++){
            timeWeights.set(i, timeWeights.get(i) * 100 / maxTimeWeight);
        }

        Double valueSum = 0.0;
        for(Map<String, Object> point : dataPoints){
            Double weight;
            Object value = point.get(label);
            if(value instanceof Double) weight = Math.abs((Double) value);
            else if(value instanceof Long) weight = Math.abs(((Long) value).doubleValue());
            else if(value instanceof Integer) weight = Math.abs(((Integer) value).doubleValue());
            else if(value instanceof Boolean) throw new Exception("not support sample boolean value");
            else if(value instanceof String) throw new Exception("not support sample string value");
            else weight = Math.abs((Double) value);
            valueSum += weight;
            valuePresum.add(valueSum);
        }
        double maxValueWeight = 0.0;
        for(int i = 0; i < valuePresum.size(); i++){
            Double divident = i > 50 ? valuePresum.get(i) - valuePresum.get(i-50) : valuePresum.get(i);
            Double dividor = i > 50 ? 50L : i+1.0;
            Object value = dataPoints.get(i).get(label);
            double v;
            if(value instanceof Double) v = Math.abs((Double) value);
            else if(value instanceof Long) v = Math.abs(((Long) value).doubleValue());
            else if(value instanceof Integer) v = Math.abs(((Integer) value).doubleValue());
            else v = Math.abs((Double) value);
            double valueWeight = Math.abs(v - (divident/dividor));
            valueWeights.add(valueWeight);
            maxValueWeight = Math.max(maxValueWeight, valueWeight);
        }
        for(int i = 0; i < valueWeights.size(); i++){
            valueWeights.set(i, valueWeights.get(i) * 100 / maxValueWeight);
            dataPoints.get(i).put("Type", valueWeights.get(i) > 90 ? 2L : timeWeights.get(i) > 10 ? 1L : 0L);
        }

        for(int i = 0; i < timeWeights.size(); i++){
            weights.add(timeWeights.get(i) + valueWeights.get(i));
            dataPoints.get(i).put("weight", weights.get(i));
        }

        System.out.println("weight used " + (System.currentTimeMillis() - stime) + "ms");

        List<Map<String, Object>> datapoints = dataPoints;
        if(datapoints.size() <= k){
            return datapoints;
        }

        Set<Map<String, Object>> candi = new HashSet<>();
        Set<Integer> ids = new HashSet<>();
        Queue<BucketDataPoint> H = new PriorityQueue<>(datapoints.size(), bucketComparator);
        Queue<Integer> maxWeight = new LinkedList<>();
        Queue<Integer> minWeight = new LinkedList<>();
        for(int i = 0; i < datapoints.size(); i++){
            double weight;
            Map<String, Object> data = datapoints.get(i);
            Object value = data.get(label);
            if(value instanceof Double) weight = (Double)value;
            else if(value instanceof Integer) weight = ((Integer) value).doubleValue();
            else if(value instanceof Long) weight = ((Long) value).doubleValue();
            else weight = (Double)data.get(label);

            double sim = 0.0;
            for(int j = i; j > i - theta && j >= 0; j--) {
                double diff;
                Object v1 = datapoints.get(j).get(label);
                Object v2 = data.get(label);
                if(v1 instanceof Double) diff = ((Double) v1).doubleValue() - ((Double)v2).doubleValue();
                else if(v1 instanceof Integer) diff = ((Integer) v1).doubleValue() - ((Integer)v2).doubleValue();
                else if(v1 instanceof Long) diff = ((Long) v1).doubleValue() - ((Long)v2).doubleValue();
                else diff = ((Double) v1).doubleValue() - ((Double)v2).doubleValue();
                sim = Math.max(Math.abs(diff), sim);
            }
            if(!maxWeight.isEmpty() && i - maxWeight.peek() >= theta) maxWeight.poll();
            if(!minWeight.isEmpty() && i - minWeight.peek() >= theta) minWeight.poll();

            while (!maxWeight.isEmpty()){
                Object v1 = datapoints.get(maxWeight.peek()).get(label);
                double v;
                if(v1 instanceof Double) v = ((Double) v1).doubleValue();
                else if(v1 instanceof Integer) v = ((Integer) v1).doubleValue();
                else if(v1 instanceof Long) v = ((Long) v1).doubleValue();
                else v = ((Double) v1).doubleValue();
                if(weight >= v) maxWeight.poll();
                else break;
            }
            maxWeight.offer(i);
            while (!minWeight.isEmpty()){
                Object v1 = datapoints.get(minWeight.peek()).get(label);
                double v;
                if(v1 instanceof Double) v = ((Double) v1).doubleValue();
                else if(v1 instanceof Integer) v = ((Integer) v1).doubleValue();
                else if(v1 instanceof Long) v = ((Long) v1).doubleValue();
                else v = ((Double) v1).doubleValue();
                if(weight >= v) minWeight.poll();
                else break;
            }
            minWeight.offer(i);

            Object value1 = datapoints.get(maxWeight.peek()).get(label);
            double maxValue, minValue;
            if(value1 instanceof Double) maxValue = (Double)value1;
            else if(value1 instanceof Integer) maxValue = ((Integer) value1).doubleValue();
            else if(value1 instanceof Long) maxValue = ((Long) value1).doubleValue();
            else maxValue = (Double)value1;

            Object value2 = datapoints.get(minWeight.peek()).get(label);
            if(value2 instanceof Double) minValue = (Double)value2;
            else if(value2 instanceof Integer) minValue = ((Integer) value2).doubleValue();
            else if(value2 instanceof Long) minValue = ((Long) value2).doubleValue();
            else minValue = (Double)value2;
            sim = Math.max(maxValue - weight, weight - minValue);
            H.offer(new BucketDataPoint(data, i, sim));
        }
        for(int i = 0; i < amount; i++){
            BucketDataPoint c = H.poll();
            if(c == null) break;
            while (c.getIter() != candi.size()){
                if(!ids.contains(c.getId())){
                    double sim = 0;
                    for(int j = i; j > i - theta && j >= 0 && !candi.contains(datapoints.get(j)); j--) {
                        double diff;
                        Object v1 = c.getData().get(label);
                        Object v2 = datapoints.get(j).get(label);
                        if(v1 instanceof Double) diff = ((Double) v1).doubleValue() - ((Double)v2).doubleValue();
                        else if(v1 instanceof Integer) diff = ((Integer) v1).doubleValue() - ((Integer)v2).doubleValue();
                        else if(v1 instanceof Long) diff = ((Long) v1).doubleValue() - ((Long)v2).doubleValue();
                        else diff = ((Double) v1).doubleValue() - ((Double)v2).doubleValue();
                        sim = Math.max(Math.abs(diff), sim);
                    }
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

        System.out.println("sample used " + (System.currentTimeMillis() - stime) + "ms");

        if(format.equals("map")) return res;

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
