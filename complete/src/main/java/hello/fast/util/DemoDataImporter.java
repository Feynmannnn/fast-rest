package hello.fast.util;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * IoTDB示例数据导入类
 */

@RestController
public class DemoDataImporter {

    @RequestMapping("/demo")
    public String demo(
            @RequestParam(value="database") String database,
            @RequestParam(value="batch") Integer batch,
            @RequestParam(value="batchSize") Integer batchSize
    ){
        database = database.replace("\"", "");
        DemoDataThread demoDataThread = new DemoDataThread(database, batch, batchSize);
        demoDataThread.start();

        return "demo data started";
    }
}
