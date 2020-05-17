package hello;

import org.springframework.util.DigestUtils;

import java.sql.*;
import java.util.*;
import java.util.Date;

public class ErrorSubscribeThread extends Thread {

    private static Comparator<Map<String, Object>> sampleComparator = new Comparator<Map<String, Object>>(){
        @Override
        public int compare(Map<String, Object> sampleDataPoint1, Map<String, Object> sampleDataPoint2){
            long t1 = (Timestamp.valueOf(sampleDataPoint1.get("time").toString())).getTime();
            long t2 = (Timestamp.valueOf(sampleDataPoint2.get("time").toString())).getTime();
            return Math.round(t1-t2);
        }
    };

    public volatile boolean exit = false;
    private static final String salt = "&%12345***&&%%$$#@1";
    String url;
    String username;
    String password;
    String database;
    String timeseries;
    String columns;
    String starttime;
    String TYPE;
    Double percent;
    Double alpha;
    Integer ratio;
    Long interval = 300L;
    String subId;
    String latestTime = "";
    Integer level = 0;
    List<Map<String, Object>> newcomingData = new LinkedList<>();
    String dbtype;
    long bucketSum;
    final long setuptime;
    long throughput;
    long batchlimit;

    ErrorSubscribeThread(String url, String username, String password, String database, String timeseries, String columns, String starttime, String TYPE, Double percent, Double alpha, Integer ratio, String subId, Integer level, String dbtype, Long batchlimit){
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
        this.timeseries = timeseries;
        this.columns = columns;
        this.starttime = starttime;
        this.TYPE = TYPE;
        this.percent = percent;
        this.alpha = alpha;
        this.ratio = ratio;
        this.subId = subId;
        this.level = level;
        this.dbtype = dbtype;
        for(int i = 0; i < level; i++){
            interval *= 50;
        }
        this.setuptime = System.currentTimeMillis();
        this.throughput = 0L;
        this.batchlimit = batchlimit;
    }

    @Override
    public void run() {

        if(level > 2) return;

        // wait for the lower level to sample data
        try {
            Thread.sleep(interval);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        String pgDatabase = database.replace('.', '_');
        String tableName = "L" + level + "_M" + subId;
        System.out.println(tableName);

        latestTime = starttime;

        // fetch [patchLimit] rows once
        Long patchLimit = batchlimit;

        String innerUrl = "jdbc:postgresql://192.168.10.172:5432/";
        String innerUserName = "postgres";
        String innerPassword = "1111aaaa";

        PGConnection pgtool = new PGConnection(
                innerUrl,
                innerUserName,
                innerPassword
        );
        Connection connection = pgtool.getConn();

        if (connection == null) {
            System.out.println("get connection defeat");
            return;
        }

        // create database if not exist
        String createDatabaseSql = String.format("create database %s;", pgDatabase);
        System.out.println(createDatabaseSql);
        pgtool.queryUpdate(connection, createDatabaseSql);

        pgtool = new PGConnection(
                "jdbc:postgresql://192.168.10.172:5432/" + pgDatabase,
                "postgres",
                "1111aaaa"
        );
        connection = pgtool.getConn();
        String extentionsql = "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;";
        pgtool.queryUpdate(connection, extentionsql);

        // create table if not exist
        String createTableSql =
                "CREATE TABLE %s (" +
                "   time    TIMESTAMPTZ         NOT NULL," +
                "   weight  DOUBLE PRECISION    NOT NULL," +
                "   error  DOUBLE PRECISION    NOT NULL," +
                "   area  DOUBLE PRECISION    NOT NULL," +
                "   %s      %s                  NOT NULL" +
                ");";
        String createHyperTableSql = "select create_hypertable('%s', 'time');";
        System.out.println(String.format(createTableSql, tableName, columns, TYPE));
        pgtool.queryUpdate(connection, String.format(createTableSql, tableName, columns, TYPE));
        System.out.println(String.format(createHyperTableSql, tableName));
        pgtool.queryUpdate(connection, String.format(createHyperTableSql, tableName));

        // different value column name for different database
        String iotdbLabel = database + "." + timeseries + "." +columns;
        String weightLabel = dbtype.equals("iotdb") ? database + "." + timeseries + ".weight" : "weight";
        System.out.println(weightLabel);
        String label = dbtype.equals("iotdb") ? iotdbLabel : columns.toLowerCase();
        String timelabel = "time";

        // fetch first patch
//        columns = level > 0 ? columns + ",weight" : columns;
        List<Map<String, Object>> linkedDataPoints = null;
        try {
            linkedDataPoints = new DataPointController().dataPoints(
                    level > 0 ? innerUrl : url,
                    level > 0 ? innerUserName : username,
                    level > 0 ? innerPassword : password,
                    level > 0 ? pgDatabase : database,
                    timeseries,
                    level > 0 ? columns + ", weight" : columns,
                    latestTime,
                    null,
                    String.format(" limit %s", patchLimit),
                    null,
                    "map",
                    null,
                    null,
                    level > 0 ? "pg" : "iotdb");
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("datasize: " + (linkedDataPoints == null ? "null": linkedDataPoints.size()));
        List<Map<String, Object>> dataPoints = new ArrayList<>(linkedDataPoints);

        // time format change for iotdb
        for(Map<String, Object> dataPoint : dataPoints) dataPoint.put(timelabel, dataPoint.get(timelabel).toString().replace("T", " "));

        List<Bucket> buckets = GBucketController.bucketsDivide(dataPoints, timelabel, label, percent, alpha, dataPoints.size()/ratio);

        // 进行桶内采样
        List<Map<String, Object>> sampleDataPoints = M4SampleController.m4sampling(buckets, label);

        // 删除上次的最后一桶
        String deletesql = String.format("delete from %s where time>='%s'", tableName, latestTime);
        System.out.println(deletesql);
        pgtool.queryUpdate(connection, deletesql);

        throughput += dataPoints.size();
        long usedtime = System.currentTimeMillis() - setuptime;
        System.out.println(String.format("throughput:%d, used time: %d, average:%d", throughput, usedtime, throughput / usedtime * 1000));

        // sample complete, persist to iotdb
        String batchInsertFormat = "insert into %s (time, weight, error, area, %s) values ('%s', %s, %s, %s, %s);";

        Collections.sort(sampleDataPoints, sampleComparator);

        ErrorController.lineError(dataPoints, sampleDataPoints, label, true);

        List<String> sqls = new LinkedList<>();
        for(Map<String, Object> map : sampleDataPoints) {
            sqls.add(String.format(batchInsertFormat, tableName, columns, map.get("time").toString().substring(0,19), map.get("weight"), map.get("error"), map.get("area"),map.get(label)));
        }
        StringBuilder sb = new StringBuilder();
        for(String sql : sqls) sb.append(sql);
        String bigSql = sb.toString();
        pgtool.queryUpdate(connection, bigSql);

        System.out.println("batch start time: " + latestTime);
        // process the newest bucket
        newcomingData = buckets.get(buckets.size()-1).getDataPoints();
        latestTime = newcomingData.get(0).get("time").toString().substring(0,19);
        System.out.println("batch latest time: " + latestTime);

        // kick off the next level sample
        String Identifier = String.format("%s,%s,%s,%s,%s", url, database, tableName, columns, salt);
        String newSubId = DigestUtils.md5DigestAsHex(Identifier.getBytes()).substring(0,8);
        System.out.println(newSubId);
        ErrorSubscribeThread pgsubscribeThread = new ErrorSubscribeThread(url, username, password, database, tableName, columns, starttime, TYPE, percent, alpha, ratio, newSubId, level+1, "pg", batchlimit);
        pgsubscribeThread.start();

        // 生命在于留白
//        try {
//            Thread.sleep(interval);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        // keep sampling data
        while(!exit){
//            System.out.println(latestTime);

            try {
                linkedDataPoints = new DataPointController().dataPoints(
                        level > 0 ? innerUrl : url,
                        level > 0 ? innerUserName : username,
                        level > 0 ? innerPassword : password,
                        level > 0 ? pgDatabase : database,
                        timeseries,
                        level > 0 ? columns + ", weight" : columns,
                        latestTime,
                        null,
                        String.format(" limit %s", patchLimit),
                        null,
                        "map",
                        null,
                        null,
                        level > 0 ? "pg" : "iotdb");
            } catch (Exception e) {
                e.printStackTrace();
            }
            dataPoints = new ArrayList<>(newcomingData);
            dataPoints.addAll(linkedDataPoints);

            for(Map<String, Object> dataPoint : dataPoints) dataPoint.put(timelabel, dataPoint.get(timelabel).toString().replace("T", " "));

            Collections.sort(dataPoints, sampleComparator);

            // calc weights
            buckets = GBucketController.bucketsDivide(dataPoints, timelabel, label, percent, alpha, dataPoints.size()/ratio);
            System.out.println("buckets size: " + buckets.size());

            // 桶内采样
            sampleDataPoints = M4SampleController.m4sampling(buckets, label);


            // sample complete, persist to iotdb
            if(dataPoints.size() < patchLimit) {
                exit = true;
                System.out.println("Level" + level + " catch up!<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");

            }

            // 删除上次的最后一桶
            deletesql = String.format("delete from %s where time>='%s'", tableName, latestTime);
            System.out.println(deletesql);
            pgtool.queryUpdate(connection, deletesql);

            throughput += dataPoints.size();
            usedtime = System.currentTimeMillis() - setuptime;
            System.out.println(String.format("throughput:%d, used time: %d, average:%d", throughput, usedtime, throughput / usedtime * 1000));

            // 写入新一批样本
            Collections.sort(sampleDataPoints, sampleComparator);

            ErrorController.lineError(dataPoints, sampleDataPoints, label, true);

            sqls = new LinkedList<>();
            for(Map<String, Object> map : sampleDataPoints){
                sqls.add(String.format(batchInsertFormat, tableName, columns, map.get("time").toString().substring(0,19), map.get("weight"), map.get("error"), map.get("area"), map.get(label)));
            }
            sb = new StringBuilder();
            for(String sql : sqls) sb.append(sql);
            bigSql = sb.toString();
            pgtool.queryUpdate(connection, bigSql);

            if(exit) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            // 单独处理最新桶
            System.out.println("batch start time: " + latestTime);
            newcomingData = buckets.get(buckets.size()-1).getDataPoints();
            latestTime = newcomingData.get(0).get("time").toString().substring(0,19);
            System.out.println("batch latest time: " + latestTime);

            System.out.println("buckets.size():" + buckets.size());
            System.out.println(String.format("L%d data size: %s, sample size: %s, ratio: %s", level, dataPoints.size(), sampleDataPoints.size(), dataPoints.size() / sampleDataPoints.size()));

            // 生命在于留白
//            try {
//                Thread.sleep(interval);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }
}
