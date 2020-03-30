package hello;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Timestamp;
import java.util.*;

@RestController
public class WeightController {
    @RequestMapping("/weights")
    public List<Map<String, Object>> buckets(
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
            Double weight = (t.getTime() - lastTimestamp) + 0.0;
            timeWeights.add(weight);
            lastTimestamp = t.getTime();
        }

        System.out.println("percent" + percent);

        Object lastValue = dataPoints.get(0).get(label);
        for(Map<String, Object> point : dataPoints){
            Object value = point.get(label);
            double v;
            if(value instanceof Double) v = ((Double) value - (Double) lastValue);
            else if(value instanceof Long) v = (((Long) value).doubleValue() - ((Long) lastValue).doubleValue());
            else if(value instanceof Integer) v = (((Integer) value).doubleValue() - ((Integer) lastValue).doubleValue());
            else v = ((Double) value - (Double) lastValue);
            double valueWeight = (v);
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
                double v1 = valueWeights.get(i);
                double v2 = valueWeights.get(i+1);
                double w = Math.abs(t1 * v2 - t2 * v1);
                maxWeight = Math.max(w, maxWeight);
                weights.add(w);
            }
        }
        weights.add(0.0);

        for (int i = 0; i < weights.size(); i++){
            dataPoints.get(i).put("weight", weights.get(i));
        }

        System.out.println("weight used " + (System.currentTimeMillis() - time) + "ms");
        System.out.println(dataPoints.size());
        System.out.println(weights.size());

        return dataPoints;
    }
}
