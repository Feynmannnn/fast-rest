package hello.fast.util;

import hello.fast.DataController;
import hello.fast.source.InfluxDBConnection;
import org.apache.iotdb.rpc.IoTDBRPCException;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.thrift.TException;
import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

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
        } catch (SQLException | IoTDBSessionException | TException | IoTDBRPCException e) {
            e.printStackTrace();
        }

        System.out.println("datapoints.size():" + datapoints.size());

        InfluxDBConnection influxDBConnection = new InfluxDBConnection("http://192.168.10.172:8086", "root", "root", this.database, null);

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
