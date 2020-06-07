package hello.fast.util;

import hello.fast.DataController;
import hello.fast.source.IoTDBConnection;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DemoDataThread extends Thread {

    private String database;
    private int batchSize;
    private int batch;

    DemoDataThread(String database, Integer batch, Integer batchSize){
        this.database = database;
        this.batchSize = batchSize;
        this.batch = batch;
    }

    @Override
    public void run() {
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

        List<Map<String, Object>> datapoints = new ArrayList<>();
        try {
            datapoints = DataController._dataPoints(url, username, password, database, timeseries, columns, "time", starttime, endtime, conditions, query, format, ip, port, dbtype);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        System.out.println("datapoints.size():" + datapoints.size());

        String dataUrl = "jdbc:iotdb://192.168.10.172:6667/";
        String dataUsername = "root";
        String dataPassword = "root";

        Connection connection = IoTDBConnection.getConnection(dataUrl, dataUsername, dataPassword);
        if (connection == null) {
            System.out.println("get connection defeat");
            return;
        }

        String storageGroup = this.database == null ? "root.fast" : this.database;
        String datatype = "DOUBLE";
        String encoding = "PLAIN";

        // create database if not exist
        Statement statement = null;
        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            String createDatabaseSql = String.format("SET STORAGE GROUP TO %s", storageGroup);
            System.out.println(createDatabaseSql);
            statement.execute(createDatabaseSql);
        }catch (SQLException e){
            System.out.println(e.getMessage());
        }

        // create table if not exist
        try {
            String createTableSql = String.format("CREATE TIMESERIES %s.%s.%s WITH DATATYPE=%s, ENCODING=%s;", storageGroup, timeseries, columns, datatype, encoding);
            System.out.println(createTableSql);
            statement.execute(createTableSql);
        }catch (SQLException e){
            System.out.println(e.getMessage());
        }

        String label = database + "." + timeseries + "." + columns;
        System.out.println(label);

        long throughput = 0L;
        int index = 0;
        long round = 0;
        String insertSql = "insert into %s.%s(timestamp, %s) values(%s, %s);";
        long time;
        String value;

        long timeInterval = 1000000000L / batch / batchSize; // 1s 分配给 batch 中各个数据
        System.out.println("timeInterval:" + timeInterval);

        while (round < 10){

            long loopStartTime = System.currentTimeMillis();

            for(int i = 0; i < batch; i++){
                long batchStartTime = System.currentTimeMillis();
                time = batchStartTime * 1000000L;
                for(int j = 0; j < batchSize; j++){
                    Map<String, Object> p = datapoints.get(index);
                    time += timeInterval;
                    value = p.get(label).toString();
                    try {
                        statement.addBatch(String.format(insertSql, storageGroup, timeseries, columns, time, value));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    index++;
                    throughput++;
                    if(index >= datapoints.size()){
                        index = 0;
                        round++;
                    }
                }
                // send batch insert sql
                try {
                    statement.executeBatch();
                    statement.clearBatch();
                } catch (SQLException e) {
                    e.printStackTrace();
                }

                try {
                    Thread.sleep(Math.max(0, batchStartTime + (1000 / batch) - System.currentTimeMillis()));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

            Long usedTime = System.currentTimeMillis() - loopStartTime;
            System.out.println(String.format("Throughput: %s, used time: %s, average: %s", throughput, usedTime, batchSize * batch * 1000 / usedTime));
        }
    }
}
