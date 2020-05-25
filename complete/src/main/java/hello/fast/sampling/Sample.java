package hello.fast.sampling;

import hello.fast.obj.Bucket;

import java.util.*;

/**
* 随机采样算子，随机排序后获取前k个数据
*/
public class Sample implements SamplingOperator {
    @Override
    public List<Map<String, Object>> sample(List<Bucket> buckets, String timelabel, String label) {
        List<Map<String, Object>> res = new LinkedList<>();

        for(Bucket bucket : buckets){
            List<Map<String, Object>> datapoints = bucket.getDataPoints();
            if(datapoints.size() <= 4){
                res.addAll(datapoints);
                continue;
            }
            Collections.shuffle(datapoints);
            res.addAll(datapoints.subList(0, 4));
        }

        return res;
    }
}
