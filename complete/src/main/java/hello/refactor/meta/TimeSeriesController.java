package hello.refactor.meta;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import org.influxdb.dto.QueryResult;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;

import hello.refactor.source.IoTDBConnection;
import hello.refactor.source.PGConnection;
import hello.refactor.source.InfluxDBConnection;

/**
* 时间序列控制器，用于返回某个数据库中的所有时间序列
*/
@RestController
public class TimeSeriesController {

    @RequestMapping("/timeseries")
    public List<TimeSeries> timeSeries(
            @RequestParam(value="url", defaultValue = "jdbc:iotdb://127.0.0.1:6667/") String url,
            @RequestParam(value="username", defaultValue = "root") String username,
            @RequestParam(value="password", defaultValue = "root") String password,
            @RequestParam(value="database") String database,
            @RequestParam(value="ip", required = false) String ip,
            @RequestParam(value="port", required = false) String port,
            @RequestParam(value="dbtype", defaultValue = "iotdb") String dbtype
    ) throws SQLException {

        // 通过网址的GET请求其参数字符串会包含引号，需要去掉
        url = url.replace("\"", "");
        username = username.replace("\"", "");
        password = password.replace("\"", "");
        database = database.replace("\"", "");
        dbtype = dbtype.replace("\"", "");
        ip = ip == null ? null : ip.replace("\"", "");
        port = port == null ? null : port.replace("\"", "");

        List<TimeSeries> timeSeries = new LinkedList<>();

        if(dbtype.toLowerCase().equals("iotdb")){
            // 如果输入了IP与PORT参数，则URL参数被替换
            if(ip != null && port != null) url = String.format("jdbc:iotdb://%s:%s/", ip, port);

            Connection connection = IoTDBConnection.getConnection(url, username, password);
            if (connection == null) {
                System.out.println("get connection defeat");
                return null;
            }
            Statement statement = connection.createStatement();
            String sql = "SHOW TIMESERIES " + database.replace("\"", "");
            statement.execute(sql);
            ResultSet resultSet = statement.getResultSet();
            HashSet<String> set = new HashSet<>();
            if (resultSet != null) {
                final ResultSetMetaData metaData = resultSet.getMetaData();
                while (resultSet.next()) {
                    String device = resultSet.getString(1).split("\\.")[2];
                    if(!set.contains(device)){
                        timeSeries.add(new TimeSeries(device));
                        set.add(device);
                    }
                }
            }
            statement.close();
            connection.close();
            return timeSeries;
        }
        else if(dbtype.toLowerCase().equals("pg")){
            if(ip != null && port != null) url = String.format("jdbc:postgresql://%s:%s/", ip, port);
            PGConnection pgtool = new PGConnection(url+database, username, password);
            Connection myconn = pgtool.getConn();
            String sql = String.format("SELECT table_name FROM information_schema.tables WHERE table_catalog = '%s'and table_schema = 'public'", database);
            System.out.println(sql);
            ResultSet rs = pgtool.query(myconn, sql);
            while(rs.next()){
                timeSeries.add(new TimeSeries(rs.getString(1)));
            }
            myconn.close();
            return timeSeries;
        }
        else if(dbtype.toLowerCase().equals("influxdb")){
            if(ip != null && port != null) url = String.format("http://%s:%s/", ip, port);
            InfluxDBConnection influxDBConnection = new InfluxDBConnection(url, username, password, database, null);
            QueryResult res = influxDBConnection.query("show measurements");
            for(QueryResult.Result r : res.getResults()){
                for(QueryResult.Series s : r.getSeries()){
                    for(List<Object> x :s.getValues()){
                        for(Object o : x){
                            timeSeries.add(new TimeSeries(o.toString()));
                        }
                    }
                }
            }
            influxDBConnection.close();
            return timeSeries;
        }
        else return null;
    }
}
