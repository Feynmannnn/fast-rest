package hello.refactor.sampling;

import hello.refactor.obj.Bucket;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Aggregation implements Operator {
    @Override
    public List<Map<String, Object>> sample(List<Bucket> buckets, String timelabel, String label, String format) {
        List<Map<String, Object>> res = new LinkedList<>();
        long st = System.currentTimeMillis();
        System.out.println("bucketaggregation started");

        for(Bucket bucket : buckets){
            List<Map<String, Object>> datapoints = bucket.getDataPoints();
            if(datapoints.size() == 0) continue;
            double valueSum = 0;

            for(int i = 0; i < datapoints.size(); i++){
                Map<String, Object> candi = datapoints.get(i);
                Object value = candi.get(label);
                if(value instanceof Double) valueSum += (double)value;
                else if(value instanceof Integer) valueSum += (double)((Integer)value);
                else if(value instanceof Long) valueSum += (double)((Long)value);
                else valueSum += 0;
            }

            Map<String, Object> obj = new HashMap<>();
            obj.put(timelabel, datapoints.get(datapoints.size()-1).get(timelabel));
            obj.put(label, valueSum / datapoints.size());
            res.add(obj);
        }
        System.out.println("bucketaggregation used time: " + (System.currentTimeMillis() - st) + "ms");
        if(format.equals("map")) return res;

        timelabel = "time";

        List<Map<String, Object>> result = new LinkedList<>();
        for(Map<String, Object> map : res){
            Object time = map.get(timelabel);
            for(Map.Entry<String, Object> entry : map.entrySet()){
                String mapKey = entry.getKey();
                if(mapKey.equals(timelabel)) continue;
                Map<String, Object> m = new HashMap<>();
                m.put("time", time);
                m.put("label", mapKey);
                m.put("value", entry.getValue());
                result.add(m);
            }
        }
        return result;
    }
}
