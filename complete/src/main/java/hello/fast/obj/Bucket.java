package hello.fast.obj;

import java.util.List;
import java.util.Map;

/**
* 分桶类，包含每个桶内的数据点集合
*/
public class Bucket {
    private final List<Map<String, Object>> dataPoints;

    public Bucket(List<Map<String, Object>> dataPoints){
        this.dataPoints = dataPoints;
    }

    public List<Map<String, Object>> getDataPoints() {
        return dataPoints;
    }
}
