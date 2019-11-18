package hello;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;
import org.apache.iotdb.jdbc.IoTDBSQLException;

@RestController
public class M4SampleController {
    @RequestMapping("/m4sample")
    public List<Map<String, Object>> dataPoints(
            @RequestParam(value="database") String database,
            @RequestParam(value="timeseries") String timeseries,
            @RequestParam(value="columns") String columns,
            @RequestParam(value="starttime", required = false) Long starttime,
            @RequestParam(value="endtime", required = false) Long endtime,
            @RequestParam(value="conditions", required = false) String conditions
    ) throws SQLException {
        List<Bucket> buckets = new BucketController().buckets(database, timeseries, columns, starttime, endtime, conditions);
        List<Map<String, Object>> res = new LinkedList<Map<String, Object>>();
        long st = System.currentTimeMillis();
        System.out.println("m4sample started");
        String label = database.replace("\"", "") + "."
                + timeseries.replace("\"", "") + "."
                + columns.replace("\"", "");
        for(Bucket bucket : buckets){
            List<Map<String, Object>> datapoints = bucket.getDataPoints();
            if(datapoints.size() < 4) {res.addAll(datapoints); continue;}
            res.add(datapoints.get(0));
            Map<String, Object> maxi = datapoints.get(0);
            Map<String, Object> mini = datapoints.get(0);
            for(int i = 1; i < datapoints.size()-1; i++){
                Map<String, Object> candi = datapoints.get(i);
                if((Double)candi.get(label) >= (Double)maxi.get(label)) maxi = candi;
                if((Double)candi.get(label) <= (Double)mini.get(label)) mini = candi;
            }
            res.add(maxi);
            res.add(mini);
            res.add(datapoints.get(datapoints.size()-1));
        }
        System.out.println("m4sample used time: " + (System.currentTimeMillis() - st) + "ms");
        return res;
    }

}
