package hello.refactor.weight;

import java.util.List;
import java.util.Map;

public interface WeightFunction {
    void weightCalculation(List<Map<String, Object>> data);
}
