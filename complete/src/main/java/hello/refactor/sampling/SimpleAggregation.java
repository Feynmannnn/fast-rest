package hello.refactor.sampling;

import hello.Bucket;

import java.util.List;
import java.util.Map;

public class SimpleAggregation implements Operator {
    @Override
    public List<Map<String, Object>> sample(List<Bucket> buckets) {
        return null;
    }
}
