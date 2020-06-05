package hello.fast.util;

import hello.fast.DataController;
import hello.fast.LayerThread;
import hello.fast.QueryController;
import hello.fast.SampleController;
import org.springframework.util.DigestUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

/**
 * 性能测试类，测试吞吐量与性能延迟
 */
public class PerformanceTest {
    @RequestMapping("/latencytest")
    public String demo(
            @RequestParam(value="database") String database,
            @RequestParam(value="batchSize") Integer batchSize
    ) throws Exception {

        String url = "jdbc:iotdb://192.168.10.172:6667/";
        String username = "root";
        String password = "root";
        String timeseries = "1701";
        String columns = "ZT31";
        String starttime = "1971-01-01 00:00:00";

        // 启动数据写入，写入速率为 batchSize * 10
        database = database.replace("\"", "");
        DemoDataThread demoDataThread = new DemoDataThread(database, batchSize);
        demoDataThread.start();

        // 等待数据写入生成一部分历史数据
        Thread.sleep(10000);

        // 启动数据采样订阅
        String salt = "&%12345***&&%%$$#@1";
        String subId = DigestUtils.md5DigestAsHex(String.format("%s,%s,%s,%s,%s", url, database, timeseries, columns, salt).getBytes()).substring(0,8);
        System.out.println(subId);

        LayerThread subscribeThread = new LayerThread(url, username, password, database, timeseries, columns, "time", starttime, null, "DOUBLE PRECISION", 10, subId, 0, "m4", "iotdb", null, null, 100000L,null, null);
        subscribeThread.start();

        // 等待采样订阅追赶数据写入
        Thread.sleep(10000);

        long sampleLatency = 0L;
        long subLatency = 0L;


        for(int i = 0; i < 100; i++){

            SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //设置格式
            starttime = format.format(System.currentTimeMillis());                                //获得带格式的字符串
            String endtime = format.format(System.currentTimeMillis() + 2000L);                             //获得带格式的字符串

            List<Map<String, Object>> dataPoints = DataController._dataPoints(url, username, password, database, timeseries, columns, "time", starttime, endtime, null, null, "map", null, null, "iotdb");;
            List<Map<String, Object>> subSamplePoints = new QueryController().publish(url, username, password, database, timeseries, columns, "time", starttime, endtime, 500L, null, null, "iotdb", "map");
            List<Map<String, Object>> samplePoints = new SampleController().dataPoints(url, username, password, database, timeseries, columns, "time", starttime, endtime, null, null, "map", null, null, 500, "iotdb", "m4", null, null);
            long rawDataTime = (long)dataPoints.get(dataPoints.size()-1).get("timestamp");
            long sampleTime = (long)dataPoints.get(subSamplePoints.size()-1).get("timestamp");
            long subSampleTime = (long)dataPoints.get(samplePoints.size()-1).get("timestamp");

            sampleLatency += rawDataTime - sampleTime;
            subLatency += rawDataTime - subSampleTime;

            // 等待
            Thread.sleep(1000);
        }

        sampleLatency /= 100;
        subLatency /= 100;


        return "sample latency:" + sampleLatency + "sub latency:" + subLatency;
    }
}
