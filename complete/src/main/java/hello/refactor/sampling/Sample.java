package hello.refactor.sampling;

import hello.refactor.obj.Bucket;

import java.util.*;

public class Sample implements Operator {
    @Override
    public List<Map<String, Object>> sample(List<Bucket> buckets, String timelabel, String label, String format) {
        List<Map<String, Object>> res = new LinkedList<>();
        long st = System.currentTimeMillis();
        System.out.println("bucketsample started");

        for(Bucket bucket : buckets){
            List<Map<String, Object>> datapoints = bucket.getDataPoints();
            if(datapoints.size() <= 4){
                res.addAll(datapoints);
                continue;
            }
            Collections.shuffle(datapoints);
            res.addAll(datapoints.subList(0, 4));
        }

        if(format.equals("map")) return res;

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
