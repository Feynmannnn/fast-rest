package hello.refactor.obj;

import java.util.Map;

public class BucketDataPoint {
    private final Map<String, Object> data;
    private final int id;
    private int iter;
    private double sim;

    public BucketDataPoint(Map<String, Object> data, int id, double sim){
        this.data = data;
        this.iter = 0;
        this.id = id;
        this.sim = sim;
    }

    public Map<String, Object> getData() {
        return data;
    }

    public int getIter() {
        return iter;
    }

    public double getSim() {
        return sim;
    }

    public int getId() {
        return id;
    }

    public void setIter(int iter) {
        this.iter = iter;
    }

    public void setSim(double sim) {
        this.sim = sim;
    }
}
