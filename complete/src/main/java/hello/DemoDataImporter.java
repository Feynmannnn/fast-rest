package hello;

import hello.refactor.DataController;
import org.apache.iotdb.jdbc.IoTDBSQLException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public class DemoDataImporter {
    public static void main(String[] args) throws SQLException, InterruptedException {

        String url = "jdbc:iotdb://101.6.15.201:6667/";
        String username = "root";
        String password = "root";
        String database = "root.group_9";
        String timeseries = "1701";
        String columns = "ZT31";
        String starttime = "2019-08-15 00:00:00";
        String endtime = "2019-08-20 00:00:00";
        String conditions = " and ZT31 > 0 ";
        String query = null;
        String format = "map";
        String ip = null;
        String port = null;
        String dbtype = "iotdb";

        int batchSize = 10000;


        List<Map<String, Object>> datapoints = DataController._dataPoints(url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, format, ip, port, dbtype);

        Connection connection = IoTDBConnection.getConnection(url, username, password);
        if (connection == null) {
            System.out.println("get connection defeat");
            return;
        }

        String storageGroup = "root.mxw";
        String datatype = "DOUBLE";
        String encoding = "PLAIN";

        // create database if not exist
        Statement statement = connection.createStatement();
        try {
            String createDatabaseSql = String.format("SET STORAGE GROUP TO %s", database);
            System.out.println(createDatabaseSql);
            statement.execute(createDatabaseSql);
        }catch (IoTDBSQLException e){
            System.out.println(e.getMessage());
        }

        // create table if not exist
        try {
            String createTableSql = String.format("CREATE TIMESERIES %s.%s.%s WITH DATATYPE=%s, ENCODING=%s;", storageGroup, timeseries, columns, datatype, encoding);
            System.out.println(createTableSql);
            statement.execute(createTableSql);
        }catch (IoTDBSQLException e){
            System.out.println(e.getMessage());
        }

        while (true){
            int index = 0;
            int round = 0;
            String insertSql = "insert into %s.%s(timestamp, %s) values(%s, %s);";

            long time;
            String value;
            for(int i = 0; i < 10; i++){
                long batchStartTime = System.currentTimeMillis();
                for(int j = 0; j < batchSize; j++){
                    // add one insert sql
                    Map<String, Object> p = datapoints.get(index);
                    time = ((Timestamp.valueOf(p.get("time").toString())).getTime() + round * 7 * 86400 * 1000);
                    value = p.get(columns).toString();
                    statement.addBatch(String.format(insertSql, storageGroup, timeseries, columns, time, value));
                    index++;
                    if(index >= datapoints.size()){
                        index = 0;
                        round++;
                    }
                }
                // send patch insert sql
                statement.executeBatch();
                statement.clearBatch();

                Thread.sleep(batchStartTime + 1000 - System.currentTimeMillis());
            }
        }

    }
}
