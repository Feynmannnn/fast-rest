package hello.refactor.obj;

import java.util.List;
import java.util.Map;

public class Bucket {
    private final List<Map<String, Object>> dataPoints;

    public Bucket(List<Map<String, Object>> dataPoints){
        this.dataPoints = dataPoints;
    }

    public List<Map<String, Object>> getDataPoints() {
        return dataPoints;
    }
}
