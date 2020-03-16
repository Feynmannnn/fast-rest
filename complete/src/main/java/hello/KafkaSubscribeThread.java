package hello;

import org.springframework.util.DigestUtils;

import java.sql.*;
import java.util.*;
import java.util.Date;

public class KafkaSubscribeThread extends Thread {

    private static Comparator<Map<String, Object>> sampleComparator = new Comparator<Map<String, Object>>(){
        @Override
        public int compare(Map<String, Object> sampleDataPoint1, Map<String, Object> sampleDataPoint2){
            long t1 = Long.parseLong(sampleDataPoint1.get("time").toString());
            long t2 = Long.parseLong(sampleDataPoint2.get("time").toString());
            return Math.round(t1-t2);
        }
    };

    private static final String salt = "&%12345***&&%%$$#@1";
    String url;
    String username;
    String password;
    String database;
    String timeseries;
    String columns;
    String starttime;
    String TYPE;
    Integer theta;
    Integer k;
    Integer ratio;
    Long interval = 300L;
    String subId;
    String latestTime = "";
    Integer level = 0;
    List<Map<String, Object>> newcomingData = new LinkedList<>();
    String dbtype;
    long bucketSum;

    KafkaSubscribeThread(String url, String username, String password, String database, String timeseries, String columns, String starttime, String TYPE, Integer theta, Integer k, Integer ratio, String subId, Integer level, String dbtype){
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
        this.timeseries = timeseries;
        this.columns = "value";
        this.starttime = starttime;
        this.TYPE = TYPE;
        this.theta = theta;
        this.k = k;
        this.ratio = ratio;
        this.subId = subId;
        this.level = level;
        this.dbtype = dbtype;
        for(int i = 0; i < level; i++){
            interval *= 50;
        }
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
        Long patchLimit = 100000L;

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
                innerUrl + pgDatabase,
                innerUserName,
                innerPassword
        );
        connection = pgtool.getConn();
        String extentionsql = "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;";
        pgtool.queryUpdate(connection, extentionsql);

        // create table if not exist
        String createTableSql =
                "CREATE TABLE %s (" +
                        "   time    bigint         NOT NULL," +
                        "   weight  DOUBLE PRECISION    NOT NULL," +
                        "   %s      %s                  NOT NULL" +
                        ");";
        String createHyperTableSql = "select create_hypertable('%s', 'time');";
        System.out.println(String.format(createTableSql, tableName, "value", TYPE));
        pgtool.queryUpdate(connection, String.format(createTableSql, tableName, "value", TYPE));
        System.out.println(String.format(createHyperTableSql, tableName));
        pgtool.queryUpdate(connection, String.format(createHyperTableSql, tableName));

        //delete sample if already exist
        // TODO: check if the subId exists, but for now(2019/12/29) iotdb not support delete >= xxx
//        try {
//            String deletesql = String.format("delete from %s.%s.%s where time>=%s", database, tableName, columns, starttime);
//            System.out.println(deletesql);
//            statement.execute(deletesql);
//        }catch (IoTDBSQLException e){
//            System.out.println(e.getMessage());
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }

        // different value column name for different database
        String iotdbLabel = database + "." + timeseries + "." +"value";
        String weightLabel = dbtype.equals("iotdb") ? database + "." + timeseries + ".weight" : "weight";
        System.out.println(weightLabel);
        String label = "value";
        String timelabel = "time";


        List<Map<String, Object>> linkedDataPoints = null;
        try {
            linkedDataPoints = new DataPointController().dataPoints(
                    level > 0 ? innerUrl : url,
                    level > 0 ? innerUserName : username,
                    level > 0 ? innerPassword : password,
                    level > 0 ? pgDatabase : database,
                    timeseries,
                    level > 0 ? "value" + ", weight" : "value",
                    latestTime,
                    null,
                    String.format(" limit %s", patchLimit),
                    null,
                    "map",
                    null,
                    null,
                    level > 0 ? "pg" : "kafka");
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("datasize: " + (linkedDataPoints == null ? "null": linkedDataPoints.size()));
        List<Map<String, Object>> dataPoints = new ArrayList<>(linkedDataPoints);

        // time format change for iotdb
        for(Map<String, Object> dataPoint : dataPoints) dataPoint.put(timelabel, dataPoint.get(timelabel).toString().replace("T", " "));

        // first weights calc
        // TODO: js version of datatype compatible
        List<Double> weights = new ArrayList<>();
        List<Long> timeWeights = new ArrayList<>();
        List<Double> valueWeights = new ArrayList<>();
        List<Double> valuePresum = new ArrayList<>();
        long maxTimeWeight = 0;
        long lastTimestamp = Long.parseLong(dataPoints.get(0).get(timelabel).toString());
        Double valueSum = 0.0;
        double maxValueWeight = 0.0;

        if(level == 0){

            for(Map<String, Object> point : dataPoints){
                long t = Long.parseLong((point.get(timelabel).toString()));
                long weight = Math.abs(t - lastTimestamp);
                timeWeights.add(weight);
                lastTimestamp = t;
                maxTimeWeight = Math.max(maxTimeWeight, weight);
            }
            for(int i = 0; i < timeWeights.size(); i++){
                timeWeights.set(i, timeWeights.get(i) * 100 / (maxTimeWeight + 1));
            }

            for(Map<String, Object> point : dataPoints){
                Double weight = null;
                Object value = point.get(label);
                if(value instanceof Double) weight = Math.abs((Double) value);
                else if(value instanceof Long) weight = Math.abs(((Long) value).doubleValue());
                else if(value instanceof Integer) weight = Math.abs(((Integer) value).doubleValue());
                else if(value instanceof Boolean) try {
                    throw new Exception("not support sample boolean value");
                } catch (Exception e) {
                    e.printStackTrace();
                }
                else if(value instanceof String) try {
                    throw new Exception("not support sample string value");
                } catch (Exception e) {
                    e.printStackTrace();
                }
                else weight = Math.abs((Double) value);
                valueSum += weight;
                valuePresum.add(valueSum);
            }
            for(int i = 0; i < valuePresum.size(); i++){
                Double divident = i > 50 ? valuePresum.get(i) - valuePresum.get(i-50) : valuePresum.get(i);
                Double dividor = i > 50 ? 50L : i+1.0;
                Object value = dataPoints.get(i).get(label);
                double v;
                if(value instanceof Double) v = Math.abs((Double) value);
                else if(value instanceof Long) v = Math.abs(((Long) value).doubleValue());
                else if(value instanceof Integer) v = Math.abs(((Integer) value).doubleValue());
                else v = Math.abs((Double) value);
                double valueWeight = Math.abs(v - (divident/dividor));
                valueWeights.add(valueWeight);
                maxValueWeight = Math.max(maxValueWeight, valueWeight);
            }
            for(int i = 0; i < valueWeights.size(); i++){
                valueWeights.set(i, valueWeights.get(i) * 100 / maxValueWeight);
                dataPoints.get(i).put("Type", valueWeights.get(i) > 90 ? 2L : timeWeights.get(i) > 10 ? 1L : 0L);
            }

            for(int i = 0; i < timeWeights.size(); i++){
                weights.add(timeWeights.get(i) + valueWeights.get(i));
                dataPoints.get(i).put("weight", weights.get(i));
            }
        }
        else{
            weights = new ArrayList<>();
            for(Map<String, Object> dataPoint : dataPoints){
                weights.add((double)dataPoint.get(weightLabel));
                dataPoint.put("weight", dataPoint.get(weightLabel));
            }
        }

        // 二分查找合适的桶权重和bucketSum
        List<Bucket> buckets = new LinkedList<>();
        int n = dataPoints.size() / (ratio * k);
        long lo = 0, hi = 200 * weights.size();
        while (lo < hi){
            long mid = lo + (hi - lo >> 1);
            int count = 0;
            double sum = 0;
            for (double weight : weights) {
                if (sum + weight > mid) {
                    sum = weight;
                    if (++count > n) break;
                } else sum += weight;
            }
            count++;
            if(count >= n) lo = mid + 1;
            else hi = mid;
        }

        // bucketSum确定，进行分桶
        bucketSum = lo;
        System.out.println("bucketSum" + bucketSum);
        double sum = 0;
        int lastIndex = 0;
        for(int i = 0; i < weights.size(); i++){
            double weight = weights.get(i);
            if(sum + weight > bucketSum){
                buckets.add(new Bucket(dataPoints.subList(lastIndex, i)));
                lastIndex = i;
                sum = weight;
            }
            else sum += weight;
        }
        buckets.add(new Bucket(dataPoints.subList(lastIndex, dataPoints.size())));

        // 进行桶内采样
        List<Map<String, Object>> sampleDataPoints = new LinkedList<>();
        long st = System.currentTimeMillis();
        System.out.println("bucketsample started");
        for(Bucket bucket : buckets){
            List<Map<String, Object>> datapoints = bucket.getDataPoints();
            if(datapoints.size() <= k){
                sampleDataPoints.addAll(datapoints);
                continue;
            }

            Set<Map<String, Object>> candi = new HashSet<>();
            Set<Integer> ids = new HashSet<>();
            Queue<BucketDataPoint> H = new PriorityQueue<>(datapoints.size(), BucketSampleController.bucketComparator);
            for(int i = 0; i < datapoints.size(); i++){
                Map<String, Object> data = datapoints.get(i);
                double sim = (Double) data.get("weight") + 0.0;
                H.offer(new BucketDataPoint(data, i, sim));
            }
            for(int i = 0; i < k; i++){
                BucketDataPoint c = H.poll();
                if(c == null) break;
                while (c.getIter() != candi.size()){
                    if(!ids.contains(c.getId())){
                        c.setIter(candi.size());
                        H.offer(c);
                    }
                    c = H.poll();
                    if(c == null) break;
                }
                if(c == null) break;
                candi.add(c.getData());
                int id = c.getId();
                for(int j = id; j > id - theta && j >= 0; j--){
                    ids.add(j);
                }
            }
            sampleDataPoints.addAll(candi);
        }
        System.out.println("bucketsample used time: " + (System.currentTimeMillis() - st) + "ms");

        // 删除上次的最后一桶
        String deletesql = String.format("delete from %s where time>=%s", tableName, latestTime);
        System.out.println(deletesql);
        pgtool.queryUpdate(connection, deletesql);

        // sample complete, persist to iotdb
        String batchInsertFormat = "insert into %s (time, weight, %s) values (%s, %s, %s);";

        Collections.sort(sampleDataPoints, sampleComparator);

        List<String> sqls = new LinkedList<>();
        for(Map<String, Object> map : sampleDataPoints){
            sqls.add(String.format(batchInsertFormat, tableName, "value", map.get("time").toString(), map.get("weight"), map.get(label)));
        }
        StringBuilder sb = new StringBuilder();
        for(String sql : sqls) sb.append(sql);
        String bigSql = sb.toString();
        pgtool.queryUpdate(connection, bigSql);


        // process the newest bucket
        newcomingData = buckets.get(buckets.size()-1).getDataPoints();
        latestTime = newcomingData.get(0).get("time").toString();

        // kick off the next level sample
        String Identifier = String.format("%s,%s,%s,%s,%s", url, database, tableName, "value", salt);
        String newSubId = DigestUtils.md5DigestAsHex(Identifier.getBytes()).substring(0,8);
        System.out.println(newSubId);
//        PGSubscribeThread pgsubscribeThread = new PGSubscribeThread(url, username, password, database, tableName, "value", starttime, TYPE, theta, k, ratio, newSubId, level+1, "pg", patchLimit);
//        pgsubscribeThread.start();

        // 生命在于留白
        try {
            Thread.sleep(interval);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // keep sampling data
        while(true){
//            System.out.println(latestTime);

            try {
                linkedDataPoints = new DataPointController().dataPoints(
                        level > 0 ? innerUrl : url,
                        level > 0 ? innerUserName : username,
                        level > 0 ? innerPassword : password,
                        level > 0 ? pgDatabase : database,
                        timeseries,
                        level > 0 ? "value" + ", weight" : "value",
                        latestTime,
                        null,
                        String.format(" limit %s", patchLimit),
                        null,
                        "map",
                        null,
                        null,
                        level > 0 ? "pg" : "kafka");
            } catch (Exception e) {
                e.printStackTrace();
            }
            dataPoints = new ArrayList<>(newcomingData);
            dataPoints.addAll(linkedDataPoints);

            for(Map<String, Object> dataPoint : dataPoints) dataPoint.put(timelabel, dataPoint.get(timelabel).toString().replace("T", " "));

            Collections.sort(dataPoints, sampleComparator);

            // calc weights
            if(level == 0){
                weights = new ArrayList<>();
                timeWeights = new ArrayList<>();
                valueWeights = new ArrayList<>();
                valuePresum = new ArrayList<>();
                buckets = new LinkedList<>();

                maxTimeWeight = 0;
                lastTimestamp = Long.parseLong((dataPoints.get(0).get(timelabel).toString()));
                for(Map<String, Object> point : dataPoints){
                    long t = Long.parseLong((point.get(timelabel).toString()));
                    long weight = Math.abs(t - lastTimestamp);
                    timeWeights.add(weight);
                    lastTimestamp = t;
                    maxTimeWeight = Math.max(maxTimeWeight, weight);
                }
                for(int i = 0; i < timeWeights.size(); i++){
                    timeWeights.set(i, timeWeights.get(i) * 100 / (maxTimeWeight + 1));
                }

                valueSum = 0.0;
                for(Map<String, Object> point : dataPoints){
                    Double weight = null;
                    Object value = point.get(label);
                    if(value instanceof Double) weight = Math.abs((Double) value);
                    else if(value instanceof Long) weight = Math.abs(((Long) value).doubleValue());
                    else if(value instanceof Integer) weight = Math.abs(((Integer) value).doubleValue());
                    else if(value instanceof Boolean) try {
                        throw new Exception("not support sample boolean value");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    else if(value instanceof String) try {
                        throw new Exception("not support sample string value");
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    else weight = Math.abs((Double) value);
                    valueSum += weight;
                    valuePresum.add(valueSum);
                }
                maxValueWeight = 0.0;
                for(int i = 0; i < valuePresum.size(); i++){
                    Double divident = i > 50 ? valuePresum.get(i) - valuePresum.get(i-50) : valuePresum.get(i);
                    Double dividor = i > 50 ? 50L : i+1.0;
                    Object value = dataPoints.get(i).get(label);
                    double v;
                    if(value instanceof Double) v = Math.abs((Double) value);
                    else if(value instanceof Long) v = Math.abs(((Long) value).doubleValue());
                    else if(value instanceof Integer) v = Math.abs(((Integer) value).doubleValue());
                    else v = Math.abs((Double) value);
                    double valueWeight = Math.abs(v - (divident/dividor));
                    valueWeights.add(valueWeight);
                    maxValueWeight = Math.max(maxValueWeight, valueWeight);
                }
                for(int i = 0; i < valueWeights.size(); i++){
                    valueWeights.set(i, valueWeights.get(i) * 100 / maxValueWeight);
                    dataPoints.get(i).put("Type", valueWeights.get(i) > 90 ? 2L : timeWeights.get(i) > 10 ? 1L : 0L);
                }

                for(int i = 0; i < timeWeights.size(); i++){
                    weights.add(timeWeights.get(i) + valueWeights.get(i));
                    dataPoints.get(i).put("weight", weights.get(i));
                }
            }
            else{
                weights = new ArrayList<>();
                for(Map<String, Object> dataPoint : dataPoints){
                    weights.add((double)dataPoint.get(weightLabel));
                    dataPoint.put("weight", dataPoint.get(weightLabel));
                }
            }

            buckets = new LinkedList<>();

            // divide buckets
            sum = 0;
            lastIndex = 0;
            for(int i = 0; i < weights.size(); i++){
                double weight = weights.get(i);
                if(sum + weight > bucketSum){
                    buckets.add(new Bucket(dataPoints.subList(lastIndex, i)));
                    lastIndex = i;
                    sum = weight;
                }
                else sum += weight;
            }
            buckets.add(new Bucket(dataPoints.subList(lastIndex, dataPoints.size())));


            // 桶内采样
            sampleDataPoints = new LinkedList<>();
            st = System.currentTimeMillis();
            for(Bucket bucket : buckets){
                List<Map<String, Object>> datapoints = bucket.getDataPoints();
                if(datapoints.size() <= k){
                    System.out.println("meet <= " + k);
                    sampleDataPoints.addAll(datapoints);
                    continue;
                }

                Set<Map<String, Object>> candi = new HashSet<>();
                Set<Integer> ids = new HashSet<>();
                Queue<BucketDataPoint> H = new PriorityQueue<>(datapoints.size(), BucketSampleController.bucketComparator);
                for(int i = 0; i < datapoints.size(); i++){
                    Map<String, Object> data = datapoints.get(i);
                    double sim = (Double) data.get("weight") + 0.0;
                    H.offer(new BucketDataPoint(data, i, sim));
                }
                for(int i = 0; i < k; i++){
                    BucketDataPoint c = H.poll();
                    if(c == null) break;
                    while (c.getIter() != candi.size()){
                        if(!ids.contains(c.getId())){
                            c.setIter(candi.size());
                            H.offer(c);
                        }
                        c = H.poll();
                        if(c == null) break;
                    }
                    if(c == null) break;
                    candi.add(c.getData());
                    int id = c.getId();
                    for(int j = id; j > id - theta && j >= 0; j--){
                        ids.add(j);
                    }
                }
                sampleDataPoints.addAll(candi);
            }


            // sample complete, persist to iotdb
            if(sampleDataPoints.size() == 0) {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }

            // 删除上次的最后一桶
            deletesql = String.format("delete from %s where time>=%s", tableName, latestTime);
            System.out.println(deletesql);
            pgtool.queryUpdate(connection, deletesql);

            // 写入新一批样本
            Collections.sort(sampleDataPoints, sampleComparator);

            sqls = new LinkedList<>();
            for(Map<String, Object> map : sampleDataPoints){
                sqls.add(String.format(batchInsertFormat, tableName, "value", map.get("time").toString(), map.get("weight"), map.get(label)));
            }
            sb = new StringBuilder();
            for(String sql : sqls) sb.append(sql);
            bigSql = sb.toString();
            pgtool.queryUpdate(connection, bigSql);

            // 单独处理最新桶
            newcomingData = buckets.get(buckets.size()-1).getDataPoints();
            latestTime = newcomingData.get(0).get("time").toString();

            System.out.println("buckets.size():" + buckets.size());
            System.out.println(String.format("L%d data size: %s, sample size: %s, ratio: %s", level, dataPoints.size(), sampleDataPoints.size(), dataPoints.size() / sampleDataPoints.size()));

            // 生命在于留白
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
