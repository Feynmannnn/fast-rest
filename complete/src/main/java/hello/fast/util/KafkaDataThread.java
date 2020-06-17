package hello.fast.util;

import com.alibaba.fastjson.JSONObject;
import hello.fast.DataController;
import org.apache.iotdb.rpc.IoTDBRPCException;
import org.apache.iotdb.session.IoTDBSessionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.thrift.TException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class KafkaDataThread extends Thread {

    private String database;
    private int batchSize;
    private int batch;

    KafkaDataThread(String database, int batch, int batchSize){
        this.batch = batch;
        this.database = database;
        this.batchSize = batchSize;
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

        long throughput = 0L;
        long time;
        Double value;

        Properties properties = new Properties();
        properties.put("bootstrap.servers", jsonObject.getString("KafkaURL"));
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.DoubleSerializer");

        KafkaProducer<Long, Double> kafkaProducer = new KafkaProducer<>(properties);

        System.out.println("topic: " + this.database);
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
                    kafkaProducer.send(new ProducerRecord<>(this.database, time, value));
                    throughput++;
                }

                try {
                    Thread.sleep(Math.max(0, batchStartTime + (1000 / batch) - System.currentTimeMillis()));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

            Long usedTime = System.currentTimeMillis() - loopStartTime;
            System.out.println(String.format("Throughput: %s, used time: %s, average: %s", throughput * 10, usedTime, batchSize * 10 * 1000 / usedTime));
        }
    }
}
