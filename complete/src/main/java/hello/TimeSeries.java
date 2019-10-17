package hello;

public class TimeSeries {

    private final String timeseries;
    private final String storageGroup;
    private final String dataType;
    private final String encoding;

    TimeSeries(String timeseries, String storageGroup, String dataType, String encoding){
        this.timeseries = timeseries;
        this.storageGroup = storageGroup;
        this.dataType = dataType;
        this.encoding = encoding;
    }

    public String getTimeseries() {
        return timeseries;
    }

    public String getStorageGroup() {
        return storageGroup;
    }

    public String getDataType() {
        return dataType;
    }

    public String getEncoding() {
        return encoding;
    }
}
