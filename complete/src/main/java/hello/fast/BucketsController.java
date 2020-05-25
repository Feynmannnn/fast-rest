package hello.fast;

import hello.fast.util.OutlierDetection;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import hello.fast.obj.Bucket;

@RestController
public class BucketsController {
    @RequestMapping("/bucket")
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
            @RequestParam(value="percent", required = false) Double percent,
            @RequestParam(value="alpha", required = false) Double alpha
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

        return _buckets(url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, format, ip, port, amount, dbtype, percent, alpha);
    }

    static List<Bucket> _buckets(
            String url,
            String username,
            String password,
            String database,
            String timeseries,
            String columns,
            String starttime,
            String endtime,
            String conditions,
            String query,
            String format,
            String ip,
            String port,
            Integer amount,
            String dbtype,
            Double percent,
            Double alpha
    ) throws SQLException {
        List<Map<String, Object>> linkedDataPoints = DataController._dataPoints(
                url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, "map", ip, port, dbtype);
        if(linkedDataPoints.size() < 2) return null;

        List<Map<String, Object>> dataPoints = new ArrayList<>(linkedDataPoints);
        String iotdbLabel = database + "." + timeseries + "." +columns;
        String label = dbtype.equals("iotdb") ? iotdbLabel : columns;
        String timelabel = "time";

        return _buckets(dataPoints, timelabel, label, amount, percent, alpha);
    }

    static List<Bucket> _intervals(
            String url,
            String username,
            String password,
            String database,
            String timeseries,
            String columns,
            String starttime,
            String endtime,
            String conditions,
            String query,
            String format,
            String ip,
            String port,
            Integer amount,
            String dbtype) throws SQLException {
        List<Map<String, Object>> linkedDataPoints = DataController._dataPoints(
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

        return buckets;
    }

    static List<Bucket> _buckets(List<Map<String, Object>> dataPoints, String timelabel, String label, Integer amount, Double percent, Double alpha){
        for(Map<String, Object> dataPoint : dataPoints) dataPoint.put(timelabel, dataPoint.get(timelabel).toString().replace("T", " "));

        List<Double> weights = new ArrayList<>();
        List<Double> timeWeights = new ArrayList<>();
        List<Double> valueWeights = new ArrayList<>();
        List<Double> grads = new ArrayList<>();
        List<Bucket> res = new LinkedList<>();

        long lastTimestamp = (Timestamp.valueOf(dataPoints.get(0).get(timelabel).toString().replace("T", " ").replace("Z", ""))).getTime();
        for(Map<String, Object> point : dataPoints){
            Date t = (Timestamp.valueOf(point.get(timelabel).toString().replace("T", " ").replace("Z", "")));
            Double weight = (t.getTime() - lastTimestamp) + 0.0;
            timeWeights.add(weight);
            lastTimestamp = t.getTime();
        }

        System.out.println("percent" + percent);

        Object lastValue = dataPoints.get(0).get(label);
        Double maxValue, minValue;
        if(lastValue instanceof Double) {
            maxValue = ((Double) lastValue);
            minValue = ((Double) lastValue);
        }
        else if(lastValue instanceof Long) {
            maxValue = (((Long) lastValue).doubleValue());
            minValue = (((Long) lastValue).doubleValue());
        }
        else if(lastValue instanceof Integer) {
            maxValue = (((Integer) lastValue).doubleValue());
            minValue = (((Integer) lastValue).doubleValue());
        }
        else {
            maxValue = ((Double) lastValue);
            minValue = ((Double) lastValue);
        }

        for(Map<String, Object> point : dataPoints){
            Object value = point.get(label);
            double v;
            if(value instanceof Double) {
                v = ((Double) value - (Double) lastValue);
                maxValue = Math.max(maxValue, (Double) value);
                minValue = Math.min(minValue, (Double) value);
            }
            else if(value instanceof Long) {
                v = (((Long) value).doubleValue() - ((Long) lastValue).doubleValue());
                maxValue = Math.max(maxValue, ((Long) value).doubleValue());
                minValue = Math.min(minValue, ((Long) value).doubleValue());
            }
            else if(value instanceof Integer) {
                v = (((Integer) value).doubleValue() - ((Integer) lastValue).doubleValue());
                maxValue = Math.max(maxValue, ((Integer) value).doubleValue());
                minValue = Math.min(minValue, ((Integer) value).doubleValue());
            }
            else {
                System.out.println("label" + label);
                v = ((Double) value - (Double) lastValue);
                maxValue = Math.max(maxValue, (Double) value);
                minValue = Math.min(minValue, (Double) value);
            }
            double valueWeight = (v);
            valueWeights.add(valueWeight);
            lastValue = value;
        }
        double valueRange = maxValue - minValue;

        boolean percentIsNull = percent == null;
        boolean alphaIsNull = alpha == null;

        if(percentIsNull){
            int timeOutlierNum = OutlierDetection.zscoreOutlierNum(timeWeights, 3);
            Double[] timeWeightStat = timeWeights.toArray(new Double[0]);
            Arrays.sort(timeWeightStat);
            percent = timeWeightStat[timeWeights.size() - timeOutlierNum - 1];
            if(percent <= 0) percent = 1.0;
        }

        if(alphaIsNull){
            int valueOutlierNum = OutlierDetection.zscoreOutlierNum(valueWeights, 3);
            Double[] valueWeightStat = valueWeights.toArray(new Double[0]);
            Arrays.sort(valueWeightStat);
            alpha = valueWeightStat[valueWeights.size() - valueOutlierNum - 1];
            if(alpha <= 0) alpha = 1.0;
        }

        double grad = 0.0;
        for(int i = 1; i < dataPoints.size(); i++){
            if(timeWeights.get(i) >= percent || valueWeights.get(i) >= alpha) grad = Double.POSITIVE_INFINITY;
            else grad = Math.atan(valueWeights.get(i) / timeWeights.get(i));
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
                double t1 = timeWeights.get(i) * 100 / percent;
                double t2 = timeWeights.get(i + 1) * 100 / percent;
                double v1 = valueWeights.get(i) * 100 / alpha;
                double v2 = valueWeights.get(i + 1) * 100 / alpha;
                double AB = Math.sqrt(t1 * t1 + v1 * v1);
                double BC = Math.sqrt(t2 * t2 + v2 * v2);
                double w = (AB + BC);
                if(Double.isNaN(w)) w = 0;
                maxWeight = Math.max(w, maxWeight);
                weights.add(w);
            }
        }
        weights.add(0.0);

        for (int i = 0; i < weights.size(); i++){
            if(weights.get(i) > 0) weights.set(i, weights.get(i) * 100 / maxWeight);
            dataPoints.get(i).put("weight", weights.get(i));
        }

        System.out.println(dataPoints.size());
        System.out.println(weights.size());

        // 二分查找
        int n = amount == null ? 1000 : amount / 4;
        long lo = 0, hi = weights.size() * 100;
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

        for(int i = 0; i < res.size(); i++){
            for(Map<String, Object> p : res.get(i).getDataPoints()){
                if((Double)p.get("weight") < 0){
                    p.put("bucket", -1);
                }
                else p.put("bucket", i);
            }
        }

        System.out.println(res.size());
        return res;
    }
}
