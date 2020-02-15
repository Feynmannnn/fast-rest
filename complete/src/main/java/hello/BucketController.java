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

@RestController
public class BucketController {

    @RequestMapping("/buckets")
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
        String iotdbLabel = database + "." + timeseries + "." +columns;
        String label = dbtype.equals("iotdb") ? iotdbLabel : columns;
        String timelabel = "time";

        for(Map<String, Object> dataPoint : dataPoints) dataPoint.put(timelabel, dataPoint.get(timelabel).toString().replace("T", " "));
        long time = System.currentTimeMillis();

        List<Double> weights = new ArrayList<>();
        List<Long> timeWeights = new ArrayList<>();
        List<Double> weightFactors = new ArrayList<>();
        List<Double> valueWeights = new ArrayList<>();
        List<Double> valuePresum = new ArrayList<>();
        List<Bucket> res = new LinkedList<>();


//        String funcName = "f";
//        String functor = "function f(dataPoints, valueLabel){\n" +
//                "\t// 初始化\n" +
//                "\tvar timeWeights=[];\n" +
//                "\tvar valueWeights = [];\n" +
//                "\tvar weights = [];\n" +
//                "\tvar presum = [];\n" +
//                "\tvar maxTimeWeight = 0;\n" +
//                "\n" +
//                "\t// 计算时间权重\n" +
//                "\tvar lastTimestamp = new Date(dataPoints[0]['time']).getTime();\n" +
//                "\tfor (var i = 0; i < dataPoints.length; i++) {\n" +
//                "\t\tvar ts = new Date(dataPoints[i]['time']).getTime();\n" +
//                "\t\tvar weight = Math.abs(ts - lastTimestamp);\n" +
//                "\t\ttimeWeights.push(weight);\n" +
//                "\t\tlastTimestamp = ts;\n" +
//                "\t\tmaxTimeWeight = Math.max(weight, maxTimeWeight);\n" +
//                "\t}\n" +
//                "\tfor (var i = 0; i < timeWeights.length; i++) {\n" +
//                "\t\ttimeWeights[i] = timeWeights[i] * 100 / maxTimeWeight;\n" +
//                "\t}\n" +
//                "\n" +
//                "\t// 计算数值权重\n" +
//                "\tvar valueSum = 0.0;\n" +
//                "\tfor (var i = 0; i < dataPoints.length; i++) {\n" +
//                "\t\tvalueSum += dataPoints[i][valueLabel];\n" +
//                "\t\tpresum.push(valueSum);\n" +
//                "\t}\n" +
//                "\tvar maxValueWeight = 0.0;\n" +
//                "\tfor(var i = 0; i < presum.length; i++){\n" +
//                "\t\tvar divident = i > 50 ? presum[i] - presum[i-50] : presum[i];\n" +
//                "\t\tvar dividor = i > 50 ? 50 : i+1.0;\n" +
//                "\t\tvar avgsum = divident / dividor;\n" +
//                "\t\tvar valueWeight = Math.abs(dataPoints[i][valueLabel] - avgsum);\n" +
//                "\t\tvalueWeights.push(valueWeight);\n" +
//                "\t\tmaxValueWeight = Math.max(maxValueWeight, valueWeight);\n" +
//                "\t}\n" +
//                "\n" +
//                "\t// 合并权重\n" +
//                "\tfor (var i = 0; i < valueWeights.length; i++) {\n" +
//                "\t\tvalueWeights[i] = valueWeights[i] * 100 / maxValueWeight;\n" +
//                "\t\tdataPoints[i][\"type\"] = valueWeights[i] > 90 ? 2 : timeWeights[i] > 10 ? 1 : 0;\n" +
//                "\t\tdataPoints[i][\"weight\"] = valueWeights[i] + timeWeights[i];\n" +
//                "\t\tdataPoints[i][\"valueWeight\"] = valueWeights[i];\n" +
//                "\t\tdataPoints[i][\"timeweight\"] = timeWeights[i];\n" +
//                "\t\tweights.push(valueWeights[i] + timeWeights[i]);\n" +
//                "\t}\n" +
//                "\treturn weights;\n" +
//                "}";
//
//
//        ScriptEngine engine = new ScriptEngineManager().getEngineByName("nashorn");
//        try{
//            CompiledScript compiled = ((Compilable)engine).compile(functor);
//            compiled.eval();
//            if (engine instanceof Invocable) {
//                Invocable in = (Invocable) compiled.getEngine();
//                System.out.println("eval used " + (System.currentTimeMillis() - time) + "ms");
//                ScriptObjectMirror r = (ScriptObjectMirror)in.invokeFunction(funcName, dataPoints, label);
//                for(int i = 0; i < dataPoints.size(); i++){
//                    weights.add((Double)r.get(i + ""));
//                }
//                System.out.println("trans used " + (System.currentTimeMillis() - time) + "ms");
//            }
//        }catch(Exception e){
//            e.printStackTrace();
//        }

        long maxTimeWeight = 0;
        long lastTimestamp = (Timestamp.valueOf(dataPoints.get(0).get(timelabel).toString().replace("T", " ").replace("Z", ""))).getTime();
        long latestTimestamp = (Timestamp.valueOf(dataPoints.get(dataPoints.size()-1).get(timelabel).toString().replace("T", " ").replace("Z", ""))).getTime();
        long firstTimestamp = (Timestamp.valueOf(dataPoints.get(0).get(timelabel).toString().replace("T", " ").replace("Z", ""))).getTime();
        long timeRange = latestTimestamp - firstTimestamp;
        for(Map<String, Object> point : dataPoints){
            Date t = (Timestamp.valueOf(point.get(timelabel).toString().replace("T", " ").replace("Z", "")));
            Long weight = Math.abs(t.getTime() - lastTimestamp);
            timeWeights.add(weight);
            lastTimestamp = t.getTime();
            maxTimeWeight = Math.max(maxTimeWeight, weight);

            double factor = 0.3 + 0.7 * ((double)lastTimestamp - (double)(firstTimestamp))/ (double)(timeRange);
            weightFactors.add(factor);
        }
        for(int i = 0; i < timeWeights.size(); i++){
            timeWeights.set(i, timeWeights.get(i) * 100 / (maxTimeWeight + 1));
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
            dataPoints.get(i).put("weight", weights.get(i) * weightFactors.get(i));
        }

        System.out.println("weight used " + (System.currentTimeMillis() - time) + "ms");

        // 二分查找
        int n = amount == null ? 1000 : amount / 4;
        long lo = 0, hi = 200 * weights.size();
        while (lo < hi){
            long mid = lo + (hi - lo >> 1);
            int count = 0;
            double sum = 0;
            for (double weight : weights) {
                if (sum + weight > mid) {
                    sum = weight;
                    if (++count > n) break;
                } else sum += weight;
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
