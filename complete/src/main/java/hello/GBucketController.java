package hello;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Timestamp;
import java.util.*;

@RestController
public class GBucketController {
    @RequestMapping("/gbuckets")
    public List<Bucket> buckets(
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

        List<Map<String, Object>> linkedDataPoints = new DataPointController().dataPoints(
                url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, "map", ip, port, dbtype);
        if(linkedDataPoints.size() < 2) return null;

        List<Map<String, Object>> dataPoints = new ArrayList<>(linkedDataPoints);
        String iotdbLabel = database + "." + timeseries + "." +columns;
        String label = dbtype.equals("iotdb") ? iotdbLabel : columns;
        String timelabel = "time";

        for(Map<String, Object> dataPoint : dataPoints) dataPoint.put(timelabel, dataPoint.get(timelabel).toString().replace("T", " "));
        long time = System.currentTimeMillis();

        List<Double> weights = new ArrayList<>();
        List<Double> timeWeights = new ArrayList<>();
        List<Double> valueWeights = new ArrayList<>();
        List<Double> grads = new ArrayList<>();
        List<Bucket> res = new LinkedList<>();

        long lastTimestamp = (Timestamp.valueOf(dataPoints.get(0).get(timelabel).toString().replace("T", " ").replace("Z", ""))).getTime();
        for(Map<String, Object> point : dataPoints){
            Date t = (Timestamp.valueOf(point.get(timelabel).toString().replace("T", " ").replace("Z", "")));
            Double weight = Math.abs(t.getTime() - lastTimestamp) + 0.0;
            timeWeights.add(weight);
            lastTimestamp = t.getTime();
        }

        System.out.println("percent" + percent);

        Object lastValue = dataPoints.get(0).get(label);
        for(Map<String, Object> point : dataPoints){
            Object value = point.get(label);
            double v;
            if(value instanceof Double) v = Math.abs((Double) value - (Double) lastValue);
            else if(value instanceof Long) v = Math.abs(((Long) value).doubleValue() - ((Long) lastValue).doubleValue());
            else if(value instanceof Integer) v = Math.abs(((Integer) value).doubleValue() - ((Integer) lastValue).doubleValue());
            else v = Math.abs((Double) value - (Double) lastValue);
            double valueWeight = Math.abs(v);
            valueWeights.add(valueWeight);
            lastValue = value;
        }

        double grad = 0.0;
        for(int i = 1; i < dataPoints.size(); i++){
            if(timeWeights.get(i) >= percent || valueWeights.get(i) >= alpha) grad = Double.POSITIVE_INFINITY;
            else grad = valueWeights.get(i) / timeWeights.get(i);
            grads.add(grad);
        }
        grads.add(grad);

        Double maxWeight = 0.0;
        weights.add(0.0);
        for(int i = 1; i < dataPoints.size()-1; i++) {
            if(Double.isInfinite(grads.get(i)) || Double.isInfinite(grads.get(i-1))){
                weights.add(-1.0);
            }
            else{
                double t1 = timeWeights.get(i);
                double t2 = timeWeights.get(i+1);
                double absgrad = Math.abs(grads.get(i) - grads.get(i - 1));
                double w = absgrad * (t1 * t2);
                maxWeight = Math.max(w, maxWeight);
                weights.add(w);
                dataPoints.get(i).put("absgrad", absgrad);
                dataPoints.get(i).put("x1", t1);
                dataPoints.get(i).put("x2", t2);
            }
        }
        weights.add(0.0);

        for (int i = 0; i < weights.size(); i++){
            dataPoints.get(i).put("weight", weights.get(i));
        }

        System.out.println("weight used " + (System.currentTimeMillis() - time) + "ms");
        System.out.println(dataPoints.size());
        System.out.println(weights.size());

        // 二分查找
        int n = amount == null ? 1000 : amount / 4;
        long lo = 0, hi = weights.size() * maxWeight.longValue();
        while (lo < hi){
            long mid = lo + (hi - lo >> 1);
            int count = 0;
            double sum = 0;
            for (double weight : weights) {
                if(weight < 0){
                    if(sum > 0) count++;
                    sum = 0;
                }
                else if (sum + weight > mid) {
                    sum = weight;
                    if (++count > n) break;
                }
                else sum += weight;
            }
            count++;
            if(count >= n) lo = mid + 1;
            else hi = mid;
        }
        System.out.println("divided used " + (System.currentTimeMillis() - time) + "ms");
        long bucketSum = lo;
        System.out.println("bucketSum" + bucketSum);
        double sum = 0;
        int lastIndex = 0;

        for(int i = 0; i < weights.size(); i++){
            double weight = weights.get(i);
            if(weight < 0){
                if(sum > 0) {
                    res.add(new Bucket(dataPoints.subList(lastIndex, i)));
                    res.add(new Bucket(dataPoints.subList(i, i+1)));
                    lastIndex = i+1;
                    sum = 0;
                }
                else{
                    res.add(new Bucket(dataPoints.subList(i, i+1)));
                    lastIndex = i+1;
                    sum = 0;
                }
            }
            if(sum + weight > bucketSum){
                res.add(new Bucket(dataPoints.subList(lastIndex, i)));
                lastIndex = i;
                sum = weight;
            }
            else sum += weight;
        }
        res.add(new Bucket(dataPoints.subList(lastIndex, dataPoints.size())));
        System.out.println("buckets used " + (System.currentTimeMillis() - time) + "ms");
        System.out.println(res.size());
        return res;
    }
}
