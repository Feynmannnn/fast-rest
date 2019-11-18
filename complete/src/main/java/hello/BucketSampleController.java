package hello;

import java.util.*;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;

import org.apache.iotdb.jdbc.IoTDBSQLException;

import javax.xml.crypto.Data;

@RestController
public class BucketSampleController {

    public static Comparator<DataPoint> bucketComparator = new Comparator<DataPoint>(){
        @Override
        public int compare(DataPoint dataPoint1, DataPoint dataPoint2){
            return (int)Math.round(dataPoint1.getSim() - dataPoint2.getSim());
        }
    };

    @RequestMapping("/bucketsample")
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
        System.out.println("bucketsample started");
        String label = database.replace("\"", "") + "."
                + timeseries.replace("\"", "") + "."
                + columns.replace("\"", "");
        int theta = 50;
        for(Bucket bucket : buckets){
            List<Map<String, Object>> datapoints = bucket.getDataPoints();
            Set<Map<String, Object>> candi = new HashSet<>();
            Set<Integer> ids = new HashSet<>();
            Queue<DataPoint> H = new PriorityQueue<>(datapoints.size(), bucketComparator);
            Queue<Integer> maxWeight = new LinkedList<>();
            Queue<Integer> minWeight = new LinkedList<>();
            for(int i = 0; i < datapoints.size(); i++){
                Map<String, Object> data = datapoints.get(i);
                double weight = (Double)data.get(label);
                double sim = 0;
//                for(int j = i; j > i - theta && j >= 0; j--) sim = Math.max(Math.abs((Double)data.get(label) - (Double)datapoints.get(j).get(label)), sim);
                if(!maxWeight.isEmpty() && i - maxWeight.peek() >= theta) maxWeight.poll();
                if(!minWeight.isEmpty() && i - minWeight.peek() >= theta) minWeight.poll();
                while (!maxWeight.isEmpty() && weight >= (Double)datapoints.get(maxWeight.peek()).get(label)){
                    maxWeight.poll();
                }
                maxWeight.offer(i);
                while (!minWeight.isEmpty() && weight >= (Double)datapoints.get(minWeight.peek()).get(label)){
                    minWeight.poll();
                }
                minWeight.offer(i);
                sim = Math.max((Double)datapoints.get(maxWeight.peek()).get(label) - weight, weight - (Double)datapoints.get(minWeight.peek()).get(label));
                H.offer(new DataPoint(data, i, sim));
            }
            int k = 4;
            for(int i = 0; i < k; i++){
                DataPoint c = H.poll();
                while (c.getIter() != candi.size()){
                    if(!ids.contains(c.getId())){
                        double sim = 0;
                        for(int j = i; j > i - theta && j >= 0 && !candi.contains(datapoints.get(j)); j--) sim = Math.max(Math.abs((Double)c.getData().get(label) - (Double)datapoints.get(j).get(label)), sim);
                        c.setSim(sim);
                        c.setIter(candi.size());
                        H.offer(c);
                    }
                    c = H.poll();
                }
                candi.add(c.getData());
                int id = c.getId();
                for(int j = id; j > id - theta && j >= 0; j--){
                    ids.add(j);
                }
            }
            res.addAll(candi);
        }
        System.out.println("bucketsample used time: " + (System.currentTimeMillis() - st) + "ms");
        return res;
    }


}
