package hello.refactor;

import hello.refactor.obj.Bucket;
import hello.refactor.sampling.*;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
            @RequestParam(value="starttime", required = false) String starttime,
            @RequestParam(value="endtime", required = false) String endtime,
            @RequestParam(value="conditions", required = false) String conditions,
            @RequestParam(value="query", required = false) String query,
            @RequestParam(value="format", defaultValue = "map") String format,
            @RequestParam(value="ip", required = false) String ip,
            @RequestParam(value="port", required = false) String port,
            @RequestParam(value="amount", required = false) Integer amount,
            @RequestParam(value="dbtype", defaultValue = "iotdb") String dbtype,
            @RequestParam(value="sample", defaultValue = "m4") String sample,
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
        sample = sample.replace("\"", "");
        ip = ip == null ? null : ip.replace("\"", "");
        port = port == null ? null : port.replace("\"", "");
        query = query == null ? null : query.replace("\"", "");

        return _dataPoints(url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, format, sample, ip, port, amount, dbtype, percent, alpha);
    }

    static List<Map<String, Object>> _dataPoints(
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
            String sample,
            String ip,
            String port,
            Integer amount,
            String dbtype,
            Double percent,
            Double alpha) throws SQLException {

        List<Bucket> buckets =
            sample.contains("simple") ?
            BucketsController._intervals(url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, format, ip, port, amount, dbtype) :
            BucketsController._buckets(url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, format, ip, port, amount, dbtype, percent, alpha);

        Operator operator;

        if(sample.contains("agg")) operator = new Aggregation();
        else if(sample.contains("sample")) operator = new Sample();
        else if(sample.contains("outlier")) operator = new Outlier();
        else operator = new M4();

        String iotdbLabel = database + "." + timeseries + "." +columns;
        String label = dbtype.equals("iotdb") ? iotdbLabel : columns;
        String timelabel = "time";

        return operator.sample(buckets, timelabel, label, format);
    }

    static List<Map<String, Object>> _layerDatapoints(
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
            String sample,
            String ip,
            String port,
            Integer ratio,
            String dbtype,
            Double percent,
            Double alpha) throws SQLException {

        List<Map<String, Object>> linkedDataPoints = DataController._dataPoints(url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, format, ip, port, dbtype);
        if(linkedDataPoints.size() < 2) return null;

        List<Map<String, Object>> dataPoints = new ArrayList<>(linkedDataPoints);
        String iotdbLabel = database + "." + timeseries + "." +columns;
        String label = dbtype.equals("iotdb") ? iotdbLabel : columns;
        String timelabel = "time";

        Integer amount = dataPoints.size() / ratio;
        List<Bucket> buckets = BucketsController._buckets(dataPoints, timelabel, label, amount, percent, alpha);

        Operator operator;

        if(sample.contains("agg")) operator = new Aggregation();
        else if(sample.contains("sample")) operator = new Sample();
        else if(sample.contains("outlier")) operator = new Outlier();
        else operator = new M4();

        return operator.sample(buckets, timelabel, label, format);
    }
}
