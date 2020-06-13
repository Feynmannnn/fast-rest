package hello.fast.util;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 示例数据导入类
 */

@RestController
public class DemoDataImporter {

    @RequestMapping("/demo")
    public String demo(
            @RequestParam(value="database") String database,
            @RequestParam(value="batch") Integer batch,
            @RequestParam(value="batchSize") Integer batchSize,
            @RequestParam(value="dbtype", defaultValue = "iotdb") String dbtype

    ){
        database = database.replace("\"", "");
        dbtype = dbtype.replace("\"", "");

        if(dbtype.equals("iotdb")){
            IoTDBDataThread ioTDBDataThread = new IoTDBDataThread(database, batch, batchSize);
            ioTDBDataThread.start();
        }
        if(dbtype.equals("influxdb")){
            InfluxDBDataThread influxDBDataThread = new InfluxDBDataThread(database, batch, batchSize);
            influxDBDataThread.start();
        }
        if(dbtype.equals("postgresql") || dbtype.equals("timescaledb")){
            PGDataThread pgDataThread = new PGDataThread(database, batch, batchSize);
            pgDataThread.start();
        }
        if(dbtype.equals("kafka")){
            KafkaDataThread demoDataThread = new KafkaDataThread(database, batch, batchSize);
        demoDataThread.start();
        }

        return "demo data started";
    }
}
