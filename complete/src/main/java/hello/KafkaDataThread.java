package hello;

import hello.refactor.DataController;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.sql.*;
import java.util.*;

public class KafkaDataThread extends Thread {

    String database;

    KafkaDataThread(String database){
        this.database = database;
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

        int batchSize = 100;


        List<Map<String, Object>> linkeddatapoints = null;
        try {
            linkeddatapoints = DataController._dataPoints(url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, format, ip, port, dbtype);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        List<Map<String, Object>> datapoints = new ArrayList<>(linkeddatapoints);

        System.out.println("datapoints.size():" + datapoints.size());

        String label = database + "." + timeseries + "." + columns;
        System.out.println(label);

        Long throughput = 0L;

        int index = 0;
        long round = 0;
        long time;
        Double value;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.10.172:9093");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.DoubleSerializer");

        KafkaProducer<Long, Double> kafkaProducer = new KafkaProducer<>(properties);

        System.out.println("topic: " + this.database);

        while (round < 1000){

            long loopStartTime = System.currentTimeMillis();


            for(int i = 0; i < 10; i++){
                long batchStartTime = System.currentTimeMillis();
                time = batchStartTime;
                for(int j = 0; j < batchSize; j++){
                    Map<String, Object> p = datapoints.get(index);
                    time += 1;
//                    time = Timestamp.valueOf(p.get("time").toString()).getTime() + 5 * 84000000 * round;
                    value = Double.valueOf(p.get(label).toString());
                    kafkaProducer.send(new ProducerRecord<>(this.database, time, value));
                    System.out.println("datatime:"+time+" inserttime:"+System.currentTimeMillis());
                    index++;
                    throughput++;
                    if(index >= datapoints.size()){
                        index = 0;
                        round++;
                    }
                }

                try {
                    Thread.sleep(Math.max(0, batchStartTime + 100 - System.currentTimeMillis()));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

            Long usedTime = System.currentTimeMillis() - loopStartTime;
            System.out.println(String.format("Throughput: %s, used time: %s, average: %s", throughput * 10, usedTime, batchSize * 10 * 10000 / usedTime));
        }
    }
}
