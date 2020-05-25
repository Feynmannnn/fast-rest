package hello.fast.meta;

public class Column {
    String column;
    String dataType;
    String encoding;

    Column(String column, String dataType, String encoding){
        this.column = column;
        this.dataType = dataType;
        this.encoding = encoding;
    }

    public String getColumn() {
        return column;
    }

    public String getDataType() {
        return dataType;
    }

    public String getEncoding() {
        return encoding;
    }
}
