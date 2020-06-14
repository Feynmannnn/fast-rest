package hello.fast.util;

import hello.fast.LayerThread;
import hello.fast.QueryController;
import hello.fast.SampleController;
import org.springframework.util.DigestUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

/**
 * 性能测试类，测试吞吐量与性能延迟
 * database: 测试数据所写入的数据库名称
 * batch：每秒钟写入多少批数据
 * batchSize：每批写入数据数目
 */
@RestController
public class PerformanceTest {
    @RequestMapping("/latencytest")
    public String demo(
            @RequestParam(value="database") String database,
            @RequestParam(value="batch") Integer batch,
            @RequestParam(value="batchSize") Integer batchSize,
            @RequestParam(value="dbtype") String dbtype
    ) throws Exception {

        String url = "jdbc:iotdb://192.168.10.172:6667/";
        String username = "root";
        String password = "root";
        String timeseries = "1701";
        String columns = "ZT31";
        String starttime = "1971-01-01 00:00:00";

        // 启动数据写入
        database = database.replace("\"", "");
        dbtype = dbtype.replace("\"", "");

        Thread dataThread = null;
        if(dbtype.equals("iotdb")){
            dataThread = new IoTDBDataThread(database, batch, batchSize);
            dataThread.start();
        }
        if(dbtype.equals("influxdb")){
            dataThread = new InfluxDBDataThread(database, batch, batchSize);
            dataThread.start();
            timeseries = "m1701";
            url = "http://192.168.10.172:8086";
        }
        if(dbtype.equals("postgresql")){
            dataThread = new PGDataThread(database, batch, batchSize);
            dataThread.start();
            timeseries = "m1701";
            columns = columns.toLowerCase();
            url = "jdbc:postgresql://192.168.10.172:5432/";
            username = "postgres";
            password = "1111aaaa";
        }
        if(dbtype.equals("kafka")){
            dataThread = new KafkaDataThread(database, batch, batchSize);
            dataThread.start();
            url = "192.168.10.172:9093";
            database = "kafka";
        }


        // 等待数据写入生成一部分历史数据
        Thread.sleep(5000);

        // 启动数据采样订阅
        String salt = "&%12345***&&%%$$#@1";
        String subId = DigestUtils.md5DigestAsHex(String.format("%s,%s,%s,%s,%s", url, database, timeseries, columns, salt).getBytes()).substring(0,8);
        System.out.println(subId);

        LayerThread subscribeThread = new LayerThread(url, username, password, database, timeseries, columns, "time", starttime, null, "DOUBLE PRECISION", 10, subId, 0, "m4", dbtype, null, null, 100000L,null, null);
        subscribeThread.start();

        // 等待采样订阅追赶数据写入
        Thread.sleep(10000);

        long sampleLatency = 0L;
        long subLatency = 0L;


        for(int i = 0; i < 10; i++){

            SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"); //设置格式
            starttime = format.format(System.currentTimeMillis() - 1000L);                                //获得带格式的字符串
            String endtime = format.format(System.currentTimeMillis() + 2000L);                             //获得带格式的字符串

            List<Map<String, Object>> subSamplePoints = new QueryController().publish(url, username, password, database, timeseries, columns, "time", starttime, endtime, 500L, null, null, dbtype, "map");
            long subSampleTime = (long)subSamplePoints.get(subSamplePoints.size()-1).get("timestamp");
            long rawDataTime = (System.currentTimeMillis()+(1000/batch)) * 1000000;
            subLatency += rawDataTime - subSampleTime;

            if(!dbtype.equals("kafka")){
                List<Map<String, Object>> samplePoints = new SampleController().dataPoints(url, username, password, database, timeseries, columns, "time", starttime, endtime, null, null, "map", null, null, 500, dbtype, "m4", null, null);
                rawDataTime = (System.currentTimeMillis()+(1000/batch)) * 1000000;
                long sampleTime = (long)samplePoints.get(samplePoints.size()-1).get("timestamp");
                sampleLatency += rawDataTime - sampleTime;
            }

            // 等待
            Thread.sleep(1000);
        }

        // 取十次平均值，并转化为毫秒
        sampleLatency /= 10 * 1000000;
        subLatency /= 10 * 1000000;

        dataThread.stop();
        subscribeThread.stop();

        return "sample latency:" + sampleLatency + "ms. sub latency:" + subLatency + "ms.";
    }
}