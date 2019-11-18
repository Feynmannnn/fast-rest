package hello;

import java.util.*;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;

import org.apache.iotdb.jdbc.IoTDBSQLException;

@RestController
public class BucketController {

    @RequestMapping("/buckets")
    public List<Bucket> buckets(
            @RequestParam(value="database") String database,
            @RequestParam(value="timeseries") String timeseries,
            @RequestParam(value="columns") String columns,
            @RequestParam(value="starttime", required = false) Long starttime,
            @RequestParam(value="endtime", required = false) Long endtime,
            @RequestParam(value="conditions", required = false) String conditions
    ) throws SQLException {
        List<Map<String, Object>> linkedDataPoints = new DataPointController().dataPoints(
                database, timeseries, columns, starttime, endtime, conditions);
        List<Map<String, Object>> dataPoints = new ArrayList<>();
        dataPoints.addAll(linkedDataPoints);
        Long time = System.currentTimeMillis();
        List<Bucket> res = new LinkedList<>();
        List<Long> weights = new ArrayList<>();
        List<Long> presum = new ArrayList<>();
        String label = database.replace("\"", "") + "."
                + timeseries.replace("\"", "") + "."
                + columns.replace("\"", "");
        System.out.println(label);
        System.out.println(dataPoints.size());
        long lo = 0, hi = 0;
        for(Map<String, Object> point : dataPoints){
            Long weight = Math.abs(Math.round((Double) point.get(label)));
            lo = Math.max(lo, weight);
            weights.add(weight);
            hi += weight;
            presum.add(hi);

        }
        System.out.println(lo);
        System.out.println(hi);
        System.out.println("weight used " + (System.currentTimeMillis() - time) + "ms");
        int n = 1000;
        while (lo < hi){
            long mid = lo + (hi - lo >> 1);
            int count = 0;
            long sum = 0;
            for (long weight : weights) {
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
        long sum = 0;
        int lastIndex = 0;
        for(int i = 0; i < weights.size(); i++){
            long weight = weights.get(i);
            if(sum + weight > bucketSum){
                res.add(new Bucket(dataPoints.subList(lastIndex, i)));
                lastIndex = i;
                sum = weight;
            }
            else sum += weight;
        }
        res.add(new Bucket(dataPoints.subList(lastIndex, dataPoints.size())));
        System.out.println("buckets used " + (System.currentTimeMillis() - time) + "ms");
        return res;
    }
}
