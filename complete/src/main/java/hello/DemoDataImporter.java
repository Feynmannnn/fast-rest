package hello;

import hello.refactor.DataController;
import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@RestController
public class DemoDataImporter {

    @RequestMapping("/demo")
    public String demo(
            @RequestParam(value="database") String database
    ){
        database = database.replace("\"", "");
        DemoDataThread demoDataThread = new DemoDataThread(database);
        demoDataThread.start();

        return "demo data started";
    }
}
