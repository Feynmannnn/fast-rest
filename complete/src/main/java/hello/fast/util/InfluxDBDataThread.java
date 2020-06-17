package hello.fast.util;

import com.alibaba.fastjson.JSONObject;
import hello.fast.DataController;
import hello.fast.source.InfluxDBConnection;
import org.apache.iotdb.rpc.IoTDBRPCException;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.thrift.TException;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.sql.Time;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InfluxDBDataThread extends Thread {

    private String database;
    private int batchSize;
    private int batch;

    InfluxDBDataThread(String database, Integer batch, Integer batchSize){
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
        } catch (SQLException | IoTDBSessionException | TException | IoTDBRPCException e) {
            e.printStackTrace();
        }

        System.out.println("datapoints.size():" + datapoints.size());

        InfluxDBConnection influxDBConnection = new InfluxDBConnection(jsonObject.getString("InfluxDBURL"), jsonObject.getString("InfluxDBUsername"), jsonObject.getString("InfluxDBPassword"), this.database, null);

        long throughput = 0L;
        int index = 0;
        long round = 0;
        String insertSql = "insert into %s.%s(timestamp, %s) values(%s, %s);";
        long time;
        String value;
        String label = database + "." + timeseries + "." + columns;
        System.out.println(label);
        timeseries = "m1701";

        long timeInterval = 1000000000L / batch / batchSize; // 1s 分配给 batch 中各个数据
        System.out.println("timeInterval:" + timeInterval);

        while (round < 10){

            long loopStartTime = System.currentTimeMillis();

            for(int i = 0; i < batch; i++){
                long batchStartTime = System.currentTimeMillis();
                time = batchStartTime * 1000000L + (8 * 60 * 60 * 1000000000L);
                BatchPoints batchPoints = BatchPoints.database(this.database).build();
                for(int j = 0; j < batchSize; j++){
                    Map<String, Object> p = datapoints.get(index);
                    time += timeInterval;
                    value = p.get(label).toString();
                    // add one sql
                    batchPoints.point(Point.measurement(timeseries).time(time, TimeUnit.NANOSECONDS).addField(columns, Double.valueOf(value)).build());
                    index++;
                    throughput++;
                    if(index >= datapoints.size()){
                        index = 0;
                        round++;
                    }
                }
                // send batch insert sql
                influxDBConnection.batchInsert(batchPoints);

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
