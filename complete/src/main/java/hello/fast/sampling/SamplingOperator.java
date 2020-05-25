package hello.fast.sampling;

import hello.fast.obj.Bucket;

import java.util.List;
import java.util.Map;

public interface SamplingOperator {
    List<Map<String, Object>> sample(List<Bucket> buckets, String timelabel, String label);
}
