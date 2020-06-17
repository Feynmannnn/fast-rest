package hello.fast.util;

import com.alibaba.fastjson.JSONObject;
import hello.fast.DataController;
import hello.fast.source.InfluxDBConnection;
import hello.fast.source.IoTDBConnection;
import hello.fast.source.PGConnection;
import org.apache.iotdb.rpc.IoTDBRPCException;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.thrift.TException;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class IoTDBDataThread extends Thread {

    private String database;
    private int batchSize;
    private int batch;

    IoTDBDataThread(String database, Integer batch, Integer batchSize){
        this.database = database;
        this.batchSize = batchSize;
        this.batch = batch;
    }

    @Override
    public void run() {
        // 读取test.config文件获取数据库信息
        String config = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader("test.config"));
            String str = "";
            StringBuilder sb = new StringBuilder();
            while ((str = br.readLine()) != null) {
                str=new String(str.getBytes(),"UTF-8");//解决中文乱码问题
                sb.append(str);
            }
            config = sb.toString();
            br.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        JSONObject jsonObject = JSONObject.parseObject(config);
        String timeseries = "d1";
        String columns = "s1";

        String dataUrl = jsonObject.getString("IoTDBURL");
        String dataUsername = jsonObject.getString("IoTDBUsername");
        String dataPassword = jsonObject.getString("IoTDBPassword");

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

//        String label = database + "." + timeseries + "." + columns;
//        System.out.println(label);

        long throughput = 0L;
        String insertSql = "insert into %s.%s(timestamp, %s) values(%s, %s);";
        long time;
        double value;

        long timeInterval = 1000000000L / batch / batchSize; // 1s 分配给 batch 中各个数据
        System.out.println("timeInterval:" + timeInterval);

        while (true){

            long loopStartTime = System.currentTimeMillis();

            for(int i = 0; i < batch; i++){
                long batchStartTime = System.currentTimeMillis();
                time = batchStartTime * 1000000L;
                for(int j = 0; j < batchSize; j++){
                    time += timeInterval;
                    value = Math.random();
                    try {
                        statement.addBatch(String.format(insertSql, storageGroup, timeseries, columns, time, value));
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    throughput++;
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
