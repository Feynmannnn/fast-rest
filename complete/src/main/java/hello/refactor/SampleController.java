package hello.refactor;

import hello.Bucket;
import hello.refactor.sampling.Operator;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;

public class SampleController {
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
            @RequestParam(value="percent", defaultValue = "99996") Long percent,
            @RequestParam(value="alpha", defaultValue = "2.3") Double alpha
    ) throws Exception {
        return _dataPoints(null, null);
    }

    static List<Map<String, Object>> _dataPoints(List<Bucket> buckets, Operator operator){
        operator.sample(buckets);
        return null;
    }
}
