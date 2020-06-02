package hello.fast;

import hello.fast.obj.Bucket;
import hello.fast.sampling.*;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.util.*;

/**
* 采样控制器，将分桶后的数据进行采样
*/
@RestController
public class SampleController {
    @RequestMapping("/sample")
    public List<Map<String, Object>> dataPoints(
            @RequestParam(value="url", defaultValue = "jdbc:iotdb://127.0.0.1:6667/") String url,
            @RequestParam(value="username", defaultValue = "root") String username,
            @RequestParam(value="password", defaultValue = "root") String password,
            @RequestParam(value="database") String database,
            @RequestParam(value="timeseries") String timeseries,
            @RequestParam(value="columns") String columns,
            @RequestParam(value="timeColumn", defaultValue = "time") String timecolumn,
            @RequestParam(value="startTime", required = false) String starttime,
            @RequestParam(value="endTime", required = false) String endtime,
            @RequestParam(value="conditions", required = false) String conditions,
            @RequestParam(value="query", required = false) String query,
            @RequestParam(value="format", defaultValue = "map") String format,
            @RequestParam(value="ip", required = false) String ip,
            @RequestParam(value="port", required = false) String port,
            @RequestParam(value="amount", required = false) Integer amount,
            @RequestParam(value="dbtype", defaultValue = "iotdb") String dbtype,
            @RequestParam(value="sample", defaultValue = "m4") String sample,
            @RequestParam(value="timeLimit", required = false) Double timeLimit,
            @RequestParam(value="valueLimit", required = false) Double valueLimit
    ) throws Exception {

        url = url.replace("\"", "");
        username = username.replace("\"", "");
        password = password.replace("\"", "");
        database = database.replace("\"", "");
        timeseries = timeseries.replace("\"", "");
        columns = columns.replace("\"", "");
        timecolumn = timecolumn.replace("\"", "");
        starttime = starttime == null ? "1971-01-01 00:00:00" : starttime.replace("\"", "");
        endtime = endtime == null ? "2099-01-01 00:00:00" : endtime.replace("\"", "");
        conditions = conditions == null ? "" : conditions.replace("\"", "");
        format = format.replace("\"", "");
        dbtype = dbtype.replace("\"", "");
        sample = sample.replace("\"", "");
        ip = ip == null ? null : ip.replace("\"", "");
        port = port == null ? null : port.replace("\"", "");
        query = query == null ? null : query.replace("\"", "");

        return _samplePoints(url, username, password, database, timeseries, columns, timecolumn, starttime, endtime, conditions, query, format, sample, ip, port, amount, dbtype, timeLimit, valueLimit);
    }

    static List<Map<String, Object>> _samplePoints(
            String url,
            String username,
            String password,
            String database,
            String timeseries,
            String columns,
            String timecolumn,
            String starttime,
            String endtime,
            String conditions,
            String query,
            String format,
            String sample,
            String ip,
            String port,
            Integer amount,
            String dbtype,
            Double timeLimit,
            Double valueLimit) throws SQLException {

        List<Map<String, Object>> res = new ArrayList<>();

        String iotdbLabel = database + "." + timeseries + "." +columns;
        String label = dbtype.equals("iotdb") ? iotdbLabel : columns;
        String timelabel = "time";

        SamplingOperator samplingOperator;
        if(sample.contains("aggregation")) samplingOperator = new Aggregation();
        else if(sample.contains("random")) samplingOperator = new Sample();
        else if(sample.contains("outlier")) samplingOperator = new Outlier();
        else samplingOperator = new M4();

        long dataPointCount = DataController._dataPointsCount(url, username, password, database, timeseries, columns, timecolumn, starttime, endtime, conditions, query, format, ip, port, dbtype);

        long freememery = Runtime.getRuntime().freeMemory();
        long batchLimit = freememery * 20000L;
        if(!conditions.contains("limit")) conditions = conditions + " limit " + batchLimit;
        amount = (int)(amount * batchLimit / dataPointCount);

        String latestTime = starttime;

        while (true){
            // 先根据采样算子分桶，"simpleXXX"为等间隔分桶，否则为自适应分桶
            List<Bucket> buckets =
                sample.contains("simple") ?
                BucketsController._intervals(url, username, password, database, timeseries, columns, timecolumn, latestTime, endtime, conditions, query, format, ip, port, amount, dbtype) :
                BucketsController._buckets(url, username, password, database, timeseries, columns, timecolumn, latestTime, endtime, conditions, query, format, ip, port, amount, dbtype, timeLimit, valueLimit);

            // 无新数据，数据已经消费完成
            if(buckets == null) break;
            // 最新数据点时间没有改变，数据已经消费完成
            List<Map<String, Object>> lastBucket = buckets.get(buckets.size()-1).getDataPoints();
            String newestTime = lastBucket.get(lastBucket.size()-1).get("time").toString().substring(0, 19);
            if(latestTime.equals(newestTime)) break;
            else latestTime = newestTime;

            res.addAll(samplingOperator.sample(buckets, timelabel, label));
        }

        if(format.equals("map")) return res;
        List<Map<String, Object>> result = new LinkedList<>();
        for(Map<String, Object> map : res){
            Object time = map.get("time");
            for(Map.Entry<String, Object> entry : map.entrySet()){
                String mapKey = entry.getKey();
                if(mapKey.equals("time")) continue;
                Map<String, Object> m = new HashMap<>();
                m.put("time", time);
                m.put("label", mapKey);
                m.put("value", entry.getValue());
                result.add(m);
            }
        }
        return result;
    }

    static List<Map<String, Object>> _samplePoints(List<Bucket> buckets, String timelabel, String label, String sample){
        SamplingOperator samplingOperator;

        if(sample.contains("aggregation")) samplingOperator = new Aggregation();
        else if(sample.contains("random")) samplingOperator = new Sample();
        else if(sample.contains("outlier")) samplingOperator = new Outlier();
        else samplingOperator = new M4();

        return samplingOperator.sample(buckets, timelabel, label);
    }
}
