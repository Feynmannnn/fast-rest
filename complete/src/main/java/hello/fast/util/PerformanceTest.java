package hello.fast.util;

import hello.fast.DataController;
import hello.fast.LayerThread;
import hello.fast.QueryController;
import hello.fast.SampleController;
import org.springframework.util.DigestUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 性能测试类，测试吞吐量与性能延迟
 */
public class PerformanceTest {
    @RequestMapping("/test")
    public String demo(
            @RequestParam(value="database") String database,
            @RequestParam(value="batchSize") Integer batchSize
    ) throws InterruptedException {

        String url = "jdbc:iotdb://192.168.10.172:6667/";
        String username = "root";
        String password = "root";
        String timeseries = "1701";
        String columns = "ZT31";
        String starttime = "1971-01-01 00:00:00";

        database = database.replace("\"", "");
        DemoDataThread demoDataThread = new DemoDataThread(database, batchSize);
        demoDataThread.start();

        Thread.sleep(10000);

        String salt = "&%12345***&&%%$$#@1";
        String subId = DigestUtils.md5DigestAsHex(String.format("%s,%s,%s,%s,%s", url, database, timeseries, columns, salt).getBytes()).substring(0,8);
        System.out.println(subId);

        LayerThread subscribeThread = new LayerThread(url, username, password, database, timeseries, columns, "time", starttime, null, "DOUBLE PRECISION", 10, subId, 0, "m4", "iotdb", null, null, 100000L,null, null);
        subscribeThread.start();

//        List<Map<String, Object>> dataPoints = DataController._dataPoints();
//        List<Map<String, Object>> subSamplePoints = new QueryController().publish();
//        List<Map<String, Object>> samplePoints = new SampleController().dataPoints();
        long rawDataTime;
        long sampleTime;
        long subSampleTime;

        System.out.println("");

        return "test started";
    }
}
