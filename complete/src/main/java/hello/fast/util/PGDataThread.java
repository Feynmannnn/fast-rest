package hello.fast.util;

import com.alibaba.fastjson.JSONObject;
import hello.fast.DataController;
import hello.fast.source.PGConnection;
import org.apache.iotdb.rpc.IoTDBRPCException;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.thrift.TException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class PGDataThread extends Thread {

    private String database;
    private int batchSize;
    private int batch;

    PGDataThread(String database, Integer batch, Integer batchSize){
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

        String url = jsonObject.getString("dataURL");
        String username = jsonObject.getString("dataUsername");
        String password = jsonObject.getString("dataPassword");
        String database = jsonObject.getString("dataDatabase");
        String timeseries = jsonObject.getString("dataTimeseries");
        String columns = jsonObject.getString("dataColumns");
        String starttime = jsonObject.getString("dataStartTime");
        String endtime = jsonObject.getString("dataEndTime");
        String conditions = jsonObject.getString("dataConditions");
        String query = null;
        String format = "map";
        String ip = null;
        String port = null;
        String dbtype = jsonObject.getString("dataDbtype");

        List<Map<String, Object>> datapoints = new ArrayList<>();
        try {
            datapoints = DataController._dataPoints(url, username, password, database, timeseries, columns, "time", starttime, endtime, conditions, query, format, ip, port, dbtype);
        } catch (SQLException | IoTDBSessionException | IoTDBRPCException | TException e) {
            e.printStackTrace();
        }

        System.out.println("datapoints.size():" + datapoints.size());

        String dataUrl = jsonObject.getString("TimescaleDBURL");
        String dataUsername = jsonObject.getString("TimescaleDBUsername");
        String dataPassword = jsonObject.getString("TimescaleDBPassword");
        String TYPE = "DOUBLE PRECISION";

        PGConnection pgtool = new PGConnection(
                dataUrl,
                dataUsername,
                dataPassword
        );
        Connection connection = pgtool.getConn();

        if (connection == null) {
            System.out.println("get connection defeat");
            return;
        }

        // create database if not exist
        String createDatabaseSql = String.format("create database %s;", this.database);
            System.out.println(createDatabaseSql);
            pgtool.queryUpdate(connection, createDatabaseSql);

        pgtool = new PGConnection(
                dataUrl + this.database.toLowerCase(),
                dataUsername,
                dataPassword
        );
        connection = pgtool.getConn();

        String extentionsql = "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;";
        pgtool.queryUpdate(connection, extentionsql);

        String label = database + "." + timeseries + "." + columns;
        System.out.println(label);
        timeseries = "m1701";

        // create table
        String createTableSql =
                "CREATE TABLE %s (" +
                "   time    TIMESTAMPTZ         NOT NULL," +
                "   %s      %s                  NOT NULL" +
                ");";
        String createHyperTableSql = "select create_hypertable('%s', 'time');";
        System.out.println(String.format(createTableSql, timeseries, columns, TYPE));
        pgtool.queryUpdate(connection, String.format(createTableSql, timeseries, columns, TYPE));
        System.out.println(String.format(createHyperTableSql, timeseries));
        pgtool.queryUpdate(connection, String.format(createHyperTableSql, timeseries));


        long throughput = 0L;
        int index = 0;
        long round = 0;
        long time;
        String value;
        String batchInsertFormat = "insert into %s (time, %s) values ('%s', %s);";

        long timeInterval = 1000000000L / batch / batchSize; // 1s 分配给 batch 中各个数据
        System.out.println("timeInterval:" + timeInterval);

        String fmt = "yyyy-MM-dd HH:mm:ss.SSS";
        SimpleDateFormat sdf = new SimpleDateFormat(fmt);

        while (round < 10){

            long loopStartTime = System.currentTimeMillis();

            List<String> sqls;
            StringBuilder sb;
            String bigSql;
            String timestr;

            for(int i = 0; i < batch; i++){
                long batchStartTime = System.currentTimeMillis();
                time = batchStartTime * 1000000L;
                sqls = new ArrayList<>();
                for(int j = 0; j < batchSize; j++){
                    // add one sql
                    Map<String, Object> p = datapoints.get(index);
                    time += timeInterval;
                    timestr = (sdf.format(new Date(time/1000000L)) + "000").substring(0, 23) + (time%1000000L);
                    value = p.get(label).toString();
                    sqls.add(String.format(batchInsertFormat, timeseries, columns, timestr, value));

                    index++;
                    throughput++;
                    if(index >= datapoints.size()){
                        index = 0;
                        round++;
                    }
                }
                // send batch insert sql
                sb = new StringBuilder();
                for(String sql : sqls) {
                    if(sql.toLowerCase().contains("nan")) System.out.println(sql);
                    else sb.append(sql);
                }
                bigSql = sb.toString();
                pgtool.queryUpdate(connection, bigSql);

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
