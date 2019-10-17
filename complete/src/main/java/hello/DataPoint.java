package hello;

public class DataPoint {
    private final long timestamp;
    private final String value;

    DataPoint(long timestamp, String value){
        this.timestamp = timestamp;
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
