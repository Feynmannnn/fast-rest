package hello;

public class Column {
    String column;
    String type;
    String encoding;

    Column(String column, String type, String encoding){
        this.column = column;
        this.type = type;
        this.encoding = encoding;
    }

    public String getColumn() {
        return column;
    }

    public String getType() {
        return type;
    }

    public String getEncoding() {
        return encoding;
    }
}
