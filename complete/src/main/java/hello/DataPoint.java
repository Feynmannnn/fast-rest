package hello;

public class DataPoint {
    private final Long time;
    private final String label;
    private final Object value;

    DataPoint(Long time, String label, Object value){
        this.time = time;
        this.label = label;
        this.value = value;
    }

    public Long getTime() {
        return time;
    }

    public String getLabel() {
        return label;
    }

    public Object getValue() {
        return value;
    }
}
