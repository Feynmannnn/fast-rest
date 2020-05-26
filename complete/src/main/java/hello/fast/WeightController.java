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
public class WeightController {
    @RequestMapping("/weight")
    public List<Map<String, Object>> weights(
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
            @RequestParam(value="timeLimit", required = false) Double timeLimit,
            @RequestParam(value="valueLimit", required = false) Double valueLimit
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

        return _weights(url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, format, ip, port, amount, dbtype, timeLimit, valueLimit);
    }

    public List<Map<String, Object>> _weights(
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
            Double timeLimit,
            Double valueLimit
    ) throws SQLException {
        List<Map<String, Object>> dataPoints = DataController._dataPoints(
                url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, "map", ip, port, dbtype);

        if(dataPoints.size() < 2) return null;

        String iotdbLabel = database + "." + timeseries + "." +columns;
        String label = dbtype.equals("iotdb") ? iotdbLabel : columns;
        String timelabel = "time";

        _weights(dataPoints, timelabel, label, amount, timeLimit, valueLimit);
        
        return dataPoints;
    }
    
    public static void  _weights(List<Map<String, Object>> dataPoints, String timelabel, String label, Integer amount, Double timeLimit, Double valueLimit){
        List<Double> weights = new ArrayList<>();
        List<Double> timeWeights = new ArrayList<>();
        List<Double> valueWeights = new ArrayList<>();
        List<Double> values = new ArrayList<>();
        List<Double> grads = new ArrayList<>();

        boolean timeLimitIsNull = timeLimit == null;
        boolean valueLimitIsNull = valueLimit == null;
        
        long lastTimestamp = (Timestamp.valueOf(dataPoints.get(0).get(timelabel).toString().replace("T", " ").replace("Z", ""))).getTime();
        for (Map<String, Object> point : dataPoints) {
            Date t = (Timestamp.valueOf(point.get(timelabel).toString().replace("T", " ").replace("Z", "")));
            Double weight = (t.getTime() - lastTimestamp) + 0.0;
            timeWeights.add(weight);
            lastTimestamp = t.getTime();
        }

        Object lastValue = dataPoints.get(0).get(label);
        for (Map<String, Object> point : dataPoints) {
            Object value = point.get(label);
            double v;
            if (value instanceof Double) {
                v = ((Double) value - (Double) lastValue);
                values.add((Double)value);
            } else if (value instanceof Long) {
                v = (((Long) value).doubleValue() - ((Long) lastValue).doubleValue());
                values.add(((Long) value).doubleValue());
            } else if (value instanceof Integer) {
                v = (((Integer) value).doubleValue() - ((Integer) lastValue).doubleValue());
                values.add(((Integer) value).doubleValue());
            } else {
                System.out.println("label" + label);
                v = ((Double) value - (Double) lastValue);
                values.add((Double)value);
            }
            lastValue = value;
            valueWeights.add(v);
        }

        if(timeLimitIsNull){
            timeLimit = OutlierDetection.getMean(timeWeights) + 3 * OutlierDetection.getStdDev(timeWeights);
            System.out.println("timeLimit" + timeLimit);
            if(timeLimit <= 0) {
                Double[] timeWeightStat = timeWeights.toArray(new Double[0]);
                Arrays.sort(timeWeightStat);
                timeLimit = timeWeightStat[timeWeightStat.length * 99995 / 100000];
            }
        }

        if(valueLimitIsNull){
            
            valueLimit = 3 * OutlierDetection.getStdDev(values);
            System.out.println("valueLimit" + valueLimit);
            if(valueLimit <= 0) {
                Double[] valueWeightStat = valueWeights.toArray(new Double[0]);
                Arrays.sort(valueWeightStat);
                valueLimit = valueWeightStat[valueWeightStat.length * 99995 / 100000];
            }
        }

        System.out.println("timeLimit" + timeLimit);
        System.out.println("valueLimit" + valueLimit);

        double grad = 0.0;
        for (int i = 1; i < dataPoints.size(); i++) {
            if (timeWeights.get(i) > timeLimit || valueWeights.get(i) > valueLimit) grad = Double.POSITIVE_INFINITY;
            else grad = Math.atan(valueWeights.get(i) / timeWeights.get(i));
            grads.add(grad);
        }
        grads.add(grad);

        weights.add(0.0);
        for (int i = 1; i < dataPoints.size() - 1; i++) {
            if (Double.isInfinite(grads.get(i)) || Double.isInfinite(grads.get(i - 1))) {
                weights.add(-1.0);
            } else {
                double t1 = timeWeights.get(i) * 100  / timeLimit;
                double t2 = timeWeights.get(i + 1) * 100  / timeLimit;
                double v1 = valueWeights.get(i) * 100  / valueLimit;
                double v2 = valueWeights.get(i + 1) * 100  / valueLimit;
                double AB = Math.sqrt(t1 * t1 + v1 * v1);
                double BC = Math.sqrt(t2 * t2 + v2 * v2);
                double w = (AB + BC);
                if(Double.isNaN(w)) w = 0;
                weights.add(w);
            }
        }
        weights.add(0.0);

        for (int i = 0; i < weights.size(); i++) {
            dataPoints.get(i).put("weight", weights.get(i));
        }
    }
}
