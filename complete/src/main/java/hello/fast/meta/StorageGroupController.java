package hello.fast.meta;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.influxdb.dto.QueryResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import hello.fast.source.IoTDBConnection;
import hello.fast.source.PGConnection;
import hello.fast.source.InfluxDBConnection;

/**
* 数据库控制器，用于返回某个数据源中的所有存储组/数据库信息
*/
@RestController
public class StorageGroupController {
    @RequestMapping("/database")
    public List<StorageGroup> storageGroup(
            @RequestParam(value="url", defaultValue = "jdbc:iotdb://127.0.0.1:6667/") String url,
            @RequestParam(value="username", defaultValue = "root") String username,
            @RequestParam(value="password", defaultValue = "root") String password,
            @RequestParam(value="ip", required = false) String ip,
            @RequestParam(value="port", required = false) String port,
            @RequestParam(value="dbtype", defaultValue = "iotdb") String dbtype
    ) throws SQLException {

        // 通过网址的GET请求其参数字符串会包含引号，需要去掉
        url = url.replace("\"", "");
        username = username.replace("\"", "");
        password = password.replace("\"", "");
        dbtype = dbtype.replace("\"", "");
        ip = ip == null ? null : ip.replace("\"", "");
        port = port == null ? null : port.replace("\"", "");

        List<StorageGroup> storageGroup = new ArrayList<>();

        if(dbtype.toLowerCase().equals("iotdb")){
            // 如果输入了IP与PORT参数，则URL参数被替换
            if(ip != null && port != null) url = String.format("jdbc:iotdb://%s:%s/", ip, port);

            Connection connection = IoTDBConnection.getConnection(url, username, password);
            if (connection == null) {
                System.out.println("get connection defeat");
                return null;
            }
            Statement statement = connection.createStatement();
            String sql = "SHOW STORAGE GROUP";
            statement.execute(sql);
            ResultSet resultSet = statement.getResultSet();

            if (resultSet != null) {
                final ResultSetMetaData metaData = resultSet.getMetaData();
                while (resultSet.next()) {
                    storageGroup.add(new StorageGroup(resultSet.getString(1)));
                }
            }
            statement.close();
            connection.close();
            return storageGroup;
        }
        else if(dbtype.toLowerCase().equals("timescaledb") || dbtype.toLowerCase().equals("postgresql")){
            if(ip != null && port != null) url = String.format("jdbc:postgresql://%s:%s/", ip, port);
            PGConnection pgtool = new PGConnection(url, username, password);
            Connection connection = pgtool.getConn();
            if (connection == null) {
                System.out.println("get connection defeat");
                return null;
            }
            String sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'";
            ResultSet resultSet = pgtool.query(connection, sql);
            while(resultSet.next()){
                storageGroup.add(new StorageGroup(resultSet.getString(1)));
            }
            connection.close();
            return storageGroup;
        }
        else if(dbtype.toLowerCase().equals("influxdb")){
            if(ip != null && port != null) url = String.format("http://%s:%s/", ip, port);
            InfluxDBConnection influxDBConnection = new InfluxDBConnection(url, username, password, null, null);
            QueryResult res = influxDBConnection.query("show databases");
            for(QueryResult.Result r : res.getResults()){
                for(QueryResult.Series s : r.getSeries()){
                    for(List<Object> x :s.getValues()){
                        for(Object o : x){
                            storageGroup.add(new StorageGroup(o.toString()));
                        }
                    }
                }
            }
            return storageGroup;
        }
        else return null;
    }
}
