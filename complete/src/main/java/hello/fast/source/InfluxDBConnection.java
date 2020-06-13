package hello.fast.source;

import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

    public void createDB(String dbName) {
		influxDB.createDatabase(dbName);
	}

    public InfluxDB influxDbBuild() {
        if (influxDB == null) {
            influxDB = InfluxDBFactory.connect(url, username, password);
        }
        try {
             if (!influxDB.databaseExists(database)) {
                influxDB.createDatabase(database);
             }
        } catch (Exception e) {
            // 该数据库可能设置动态代理，不支持创建数据库
            // e.printStackTrace();
        } finally {
            influxDB.setRetentionPolicy(retentionPolicy);
        }
        influxDB.setLogLevel(InfluxDB.LogLevel.NONE);
        influxDB.enableBatch();

        return influxDB;
    }

    public QueryResult query(String command) {
        return influxDB.query(new Query(command, database));
    }

    /**
	 * 批量写入测点
	 *
	 * @param batchPoints
	 */
	public void batchInsert(BatchPoints batchPoints) {
		influxDB.write(batchPoints);
		// influxDB.enableGzip();
		// influxDB.enableBatch(2000,100,TimeUnit.MILLISECONDS);
		// influxDB.disableGzip();
		// influxDB.disableBatch();
	}

	/**
	 * 批量写入数据
	 *
	 * @param database
	 *            数据库
	 * @param retentionPolicy
	 *            保存策略
	 * @param consistency
	 *            一致性
	 * @param records
	 *            要保存的数据（调用BatchPoints.lineProtocol()可得到一条record）
	 */
	public void batchInsert(final String database, final String retentionPolicy, final InfluxDB.ConsistencyLevel consistency,
			final List<String> records) {
		influxDB.write(database, retentionPolicy, consistency, records);
	}

	/**
	 * 构建Point
	 *
	 * @param measurement
	 * @param time
	 * @param fields
	 * @return
	 */
	public Point pointBuilder(String measurement, long time, Map<String, String> tags, Map<String, Object> fields) {
		Point point = Point.measurement(measurement).time(time, TimeUnit.MILLISECONDS).tag(tags).fields(fields).build();
		return point;
	}

	public void insert(String measurement, Map<String, String> tags, Map<String, Object> fields, long time, TimeUnit timeUnit) {
        Point.Builder builder = Point.measurement(measurement);
//        builder.tag(tags);
        builder.fields(fields);
        if (0 != time) {
            builder.time(time, timeUnit);
        }
        influxDB.write(database, retentionPolicy, builder.build());
    }

    public void close() {
        influxDB.close();
    }
}