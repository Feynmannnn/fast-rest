package hello.refactor;

import org.springframework.util.DigestUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;

@RestController
public class LayerController {

    private static final String salt = "&%12345***&&%%$$#@1";

    @RequestMapping("/sub")
    public String subscribe(
            @RequestParam(value="url", defaultValue = "jdbc:iotdb://127.0.0.1:6667/") String url,
            @RequestParam(value="username", defaultValue = "root") String username,
            @RequestParam(value="password", defaultValue = "root") String password,
            @RequestParam(value="database") String database,
            @RequestParam(value="timeseries") String timeseries,
            @RequestParam(value="columns") String columns,
            @RequestParam(value="starttime") String starttime,
            @RequestParam(value="sample") String sample,
            @RequestParam(value="percent", defaultValue = "1") Double percent,
            @RequestParam(value="alpha", defaultValue = "1") Double alpha,
            @RequestParam(value="ratio", defaultValue = "20") Integer ratio,
            @RequestParam(value="ip", required = false) String ip,
            @RequestParam(value="port", required = false) String port,
            @RequestParam(value="dbtype", defaultValue = "iotdb") String dbtype,
            @RequestParam(value="batchlimit", defaultValue = "100000") Long batchlimit
    ) throws SQLException, NoSuchAlgorithmException {

        url = url.replace("\"", "");
        username = username.replace("\"", "");
        password = password.replace("\"", "");
        database = database.replace("\"", "").toLowerCase();
        timeseries = timeseries.replace("\"", "").toLowerCase();
        columns = columns.replace("\"", "").toLowerCase();
        starttime = starttime.replace("\"", "");
        sample = sample.replace("\"", "");
        ip = ip == null ? null : ip.replace("\"", "");
        port = port == null ? null : port.replace("\"", "");
        dbtype = dbtype.replace("\"", "");

        if(dbtype.toLowerCase().equals("iotdb")) {
            if (ip != null && port != null) url = String.format("jdbc:iotdb://%s:%s/", ip, port);
        }
        else if(dbtype.toLowerCase().equals("pg")) {
            if (ip != null && port != null) url = String.format("jdbc:postgresql://%s:%s/", ip, port);
        }
        else if(dbtype.toLowerCase().equals("influxdb")) {
            if (ip != null && port != null) url = String.format("http://%s:%s/", ip, port);
        }
        else{
            if (ip != null && port != null) url = String.format("jdbc:iotdb://%s:%s/", ip, port);
        }

        System.out.println(url);
        System.out.println(database);
        System.out.println(timeseries);
        System.out.println(columns);

        String subId = DigestUtils.md5DigestAsHex(String.format("%s,%s,%s,%s,%s", url, database, timeseries, columns, salt).getBytes()).substring(0,8);
        System.out.println(subId);

        String TYPE = "DOUBLE";

        switch (TYPE){
            case "INT32":
                TYPE = "integer";
                break;
            case "INT64":
                TYPE = "bigint";
                break;
            case "FLOAT":
            case "DOUBLE":
                TYPE = "DOUBLE PRECISION";
                break;
            default:
                TYPE = "text";
        }

        System.out.println(TYPE);

        LayerThread subscribeThread = new LayerThread(url, username, password, database, timeseries, columns, starttime, TYPE, ratio, subId, 0, sample,"iotdb", percent, alpha, batchlimit);
        subscribeThread.start();

        return subId;
    }

}
