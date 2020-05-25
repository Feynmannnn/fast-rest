package hello.fast.source;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

/**
 * InfluxDB数据库连接操作类
 */
public class InfluxDBConnection {

    // 用户名
    private String username;
    // 密码
    private String password;
    // 连接地址
    private String url;
    // 数据库
    private String database;
    // 保留策略
    private String retentionPolicy;

    private InfluxDB influxDB;

    public InfluxDBConnection(String url, String username, String password, String database,
                              String retentionPolicy) {
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
        this.retentionPolicy = retentionPolicy == null || retentionPolicy.equals("") ? "autogen" : retentionPolicy;
        influxDbBuild();
    }

    public InfluxDB influxDbBuild() {
        if (influxDB == null) {
            influxDB = InfluxDBFactory.connect(url, username, password);
        }
        try {
//             if (!influxDB.databaseExists(database)) {
//             influxDB.createDatabase(database);
//             }
        } catch (Exception e) {
            // 该数据库可能设置动态代理，不支持创建数据库
            // e.printStackTrace();
        } finally {
            influxDB.setRetentionPolicy(retentionPolicy);
        }
        influxDB.setLogLevel(InfluxDB.LogLevel.NONE);
        return influxDB;
    }

    public QueryResult query(String command) {
        return influxDB.query(new Query(command, database));
    }

    public void close() {
        influxDB.close();
    }
}