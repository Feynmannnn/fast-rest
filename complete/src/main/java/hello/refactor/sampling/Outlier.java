package hello.refactor.sampling;

import hello.refactor.obj.Bucket;
import hello.refactor.obj.BucketDataPoint;

import java.util.*;

public class Outlier implements Operator {

    public static Comparator<BucketDataPoint> bucketComparator = new Comparator<BucketDataPoint>(){
        @Override
        public int compare(BucketDataPoint bucketDataPoint1, BucketDataPoint bucketDataPoint2){
            return (int)-Math.round(bucketDataPoint1.getSim() - bucketDataPoint2.getSim());
        }
    };

    @Override
    public List<Map<String, Object>> sample(List<Bucket> buckets, String timelabel, String label, String format) {
        List<Map<String, Object>> res = new LinkedList<Map<String, Object>>();
        long st = System.currentTimeMillis();
        System.out.println("gradsample started");
        for(Bucket bucket : buckets){
            int theta = bucket.getDataPoints().size() / 4;
            int k = 4;
            List<Map<String, Object>> datapoints = bucket.getDataPoints();
            if(datapoints.size() <= k){
                res.addAll(datapoints);
                continue;
            }

            Set<Map<String, Object>> candi = new HashSet<>();
            Set<Integer> ids = new HashSet<>();
            Queue<BucketDataPoint> H = new PriorityQueue<>(datapoints.size(), bucketComparator);
            Queue<Integer> maxWeight = new LinkedList<>();
            Queue<Integer> minWeight = new LinkedList<>();
            for(int i = 0; i < datapoints.size(); i++){

                Map<String, Object> data = datapoints.get(i);
                double weight = (double)data.get("weight");

                double sim = 0.0;
                for(int j = i; j > i - theta && j >= 0; j--) {
                    double diff;
                    Object v1 = datapoints.get(j).get(label);
                    Object v2 = data.get(label);
                    if(v1 instanceof Double) diff = (Double) v1 - (Double) v2;
                    else if(v1 instanceof Integer) diff = ((Integer) v1).doubleValue() - ((Integer)v2).doubleValue();
                    else if(v1 instanceof Long) diff = ((Long) v1).doubleValue() - ((Long)v2).doubleValue();
                    else diff = (Double) v1 - (Double) v2;
                    sim = Math.max(Math.abs(diff), sim);
                }

                H.offer(new BucketDataPoint(data, i, sim*weight));
            }
            for(int i = 0; i < k; i++){
                BucketDataPoint c = H.poll();
                if(c == null) break;
                while (c.getIter() != candi.size()){
                    if(!ids.contains(c.getId())){
                        double sim = 0;
                        for(int j = i; j > i - theta && j >= 0 && !candi.contains(datapoints.get(j)); j--) {
                            double diff;
                            Object v1 = c.getData().get(label);
                            Object v2 = datapoints.get(j).get(label);
                            if(v1 instanceof Double) diff = (Double) v1 - (Double) v2;
                            else if(v1 instanceof Integer) diff = ((Integer) v1).doubleValue() - ((Integer)v2).doubleValue();
                            else if(v1 instanceof Long) diff = ((Long) v1).doubleValue() - ((Long)v2).doubleValue();
                            else diff = (Double) v1 - (Double) v2;
                            sim = Math.max(Math.abs(diff), sim);
                        }
                        c.setSim(sim * (double)c.getData().get("weight"));
                        c.setIter(candi.size());
                        H.offer(c);
                    }
                    c = H.poll();
                    if(c == null) break;
                }
                if(c == null) break;
                candi.add(c.getData());
                int id = c.getId();
                for(int j = id; j > id - theta && j >= 0; j--){
                    ids.add(j);
                }
            }
            res.addAll(candi);
        }
        System.out.println("gradsample used time: " + (System.currentTimeMillis() - st) + "ms");
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
