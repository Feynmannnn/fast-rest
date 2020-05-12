package hello.refactor;

import com.alibaba.fastjson.JSONObject;
import org.springframework.util.DigestUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
            @RequestParam(value="starttime", defaultValue = "1971-01-01 00:00:00") String starttime,
            @RequestParam(value="endtime", required = false) String endtime,
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
        database = database.replace("\"", "");
        timeseries = timeseries.replace("\"", "");
        columns = columns.replace("\"", "");
        starttime = starttime.replace("\"", "");
        endtime = endtime == null ? null : endtime.replace("\"", "");
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
        System.out.println(dbtype);

        String subId = DigestUtils.md5DigestAsHex(String.format("%s,%s,%s,%s,%s", url, database, timeseries, columns, salt).getBytes()).substring(0,8);
        System.out.println(subId);

        String config = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader("fast.config"));
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
        String autovisURL = jsonObject.getString("autovisURL");
        String innerUrl = jsonObject.getString("innerURL");
        String innerUserName = jsonObject.getString("innerusername");
        String innerPassword = jsonObject.getString("innerpassword");

        String L0table = "l0_m" + subId;
        System.out.println(L0table);

        String[] tables = QueryController.subTables(url, innerUrl, innerUserName, innerPassword, database, timeseries, columns);

        for(String table : tables){
            if(L0table.equals(table)) {
                return "subscribe already exists.";
            }
        }

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

        Lock lock = new ReentrantLock();
        Condition newCondition = lock.newCondition();

        LayerThread subscribeThread = new LayerThread(url, username, password, database, timeseries, columns, starttime, endtime, TYPE, ratio, subId, 0, sample, dbtype, percent, alpha, batchlimit, lock, newCondition);
        subscribeThread.start();

        return subId;
    }

}
