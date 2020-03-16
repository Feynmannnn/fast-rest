package hello.refactor.sampling;

import hello.Bucket;

import java.util.List;
import java.util.Map;

public interface Operator {
    List<Map<String, Object>> sample(List<Bucket> buckets);
}
