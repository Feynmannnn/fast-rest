package hello;

import org.apache.iotdb.jdbc.IoTDBSQLException;
import org.springframework.util.DigestUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.*;

public class SubscribeThread extends Thread {

    private static final String salt = "&%12345***&&%%$$#@1";
    String url;
    String username;
    String password;
    String database;
    String timeseries;
    String columns;
    String starttime;
    Integer theta;
    Integer k;
    Long interval = 1000L;
    String subId;
    String latestTime = "";
    Integer level = 0;
    List<Map<String, Object>> newcomingData = new LinkedList<>();
    String dbtype;
    long bucketSum;

    SubscribeThread(String url, String username, String password, String database, String timeseries, String columns, String starttime, Integer theta, Integer k, String subId, Integer level, String dbtype){
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
        this.timeseries = timeseries;
        this.columns = columns;
        this.starttime = starttime;
        this.theta = theta;
        this.k = k;
        this.subId = subId;
        this.level = level;
        this.dbtype = dbtype;
        for(int i = 0; i < level; i++){
            interval *= 30;
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

        String tableName = "L" + level + ".M" + subId;
        System.out.println(tableName);

        latestTime = starttime;

        // fetch [patchLimit] rows once
        Long patchLimit = 100000L;

        // TODO: analyse column type
        String TYPE = "INT32";
        // TODO: analyse encoding type
        String ENCODING = "PLAIN";

        // use specified iotdb as midware storage
        Connection connection = IoTDBConnection.getConnection(
                "jdbc:iotdb://101.6.15.211:6667/",
                "root",
                "root"
        );
        if (connection == null) {
            System.out.println("get connection defeat");
            return;
        }

        // create database if not exist
        Statement statement = null;
        try {
            statement = connection.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        try {
            String createDatabaseSql = String.format("SET STORAGE GROUP TO %s", database);
            System.out.println(createDatabaseSql);
            statement.execute(createDatabaseSql);
        }catch (IoTDBSQLException e){
            System.out.println(e.getMessage());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // create table if not exist
        try {
            String createTableSql = String.format("CREATE TIMESERIES %s.%s.%s WITH DATATYPE=%s, ENCODING=%s;", database, tableName, columns, TYPE, ENCODING);
            System.out.println(createTableSql);
            statement.execute(createTableSql);
        }catch (IoTDBSQLException e){
            System.out.println(e.getMessage());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        //delete sample if already exist
        // TODO: check if the subId exists
        try {
            String deletesql = String.format("delete from %s.%s.%s where time>=%s", database, tableName, columns, starttime);
            System.out.println(deletesql);
            statement.execute(deletesql);
        }catch (IoTDBSQLException e){
            System.out.println(e.getMessage());
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // different value column name for different database
        String iotdbLabel = database + "." + timeseries + "." +columns;
        String label = dbtype.equals("iotdb") ? iotdbLabel : columns;
        String timelabel = "time";

        // fetch first patch
        List<Map<String, Object>> linkedDataPoints = null;
        try {
            linkedDataPoints = new DataPointController().dataPoints(
                    url, username, password, database, timeseries, columns, latestTime, null, String.format(" limit %s", patchLimit), null, "map", null, null, "iotdb");
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
        List<Bucket> buckets = new LinkedList<>();

        long maxTimeWeight = 0;
        long lastTimestamp = (Timestamp.valueOf(dataPoints.get(0).get(timelabel).toString().replace("T", " ").replace("Z", ""))).getTime();
        for(Map<String, Object> point : dataPoints){
            Date t = (Timestamp.valueOf(point.get(timelabel).toString().replace("T", " ").replace("Z", "")));
            Long weight = Math.abs(t.getTime() - lastTimestamp);
            timeWeights.add(weight);
            lastTimestamp = t.getTime();
            maxTimeWeight = Math.max(maxTimeWeight, weight);
        }
        for(int i = 0; i < timeWeights.size(); i++){
            timeWeights.set(i, timeWeights.get(i) * 100 / (maxTimeWeight + 1));
        }

        Double valueSum = 0.0;
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
        double maxValueWeight = 0.0;
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

        // 二分查找合适的桶权重和bucketSum
        int n = dataPoints.size() / 200;
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
        ;;

        // sample complete, persist to iotdb
        String batchInsertFormat = "insert into %s.%s(timestamp, %s) values(%s, %s);";
        for(Map<String, Object> map : sampleDataPoints){
            try {
                statement.addBatch(String.format(batchInsertFormat, database, tableName, columns, map.get("time").toString().substring(0,19), map.get(label)));
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        try {
            statement.executeBatch();
            statement.clearBatch();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        // process the newest bucket
        newcomingData = buckets.get(buckets.size()-1).getDataPoints();
        latestTime = newcomingData.get(0).get("time").toString().substring(0,19);

        // kick off the next level sample
        String Identifier = String.format("%s,%s,%s,%s,%s", url, database, tableName, columns, salt);
        String newSubId = DigestUtils.md5DigestAsHex(Identifier.getBytes()).substring(0,8);
        System.out.println(newSubId);
        SubscribeThread subscribeThread = new SubscribeThread(url, username, password, database, tableName, columns, starttime, theta, k, newSubId, level+1, dbtype);
        subscribeThread.start();

        // keep sampling data
        while(true){
            System.out.println(latestTime);

            try {
                linkedDataPoints = new DataPointController().dataPoints(
                        url, username, password, database, timeseries, columns, latestTime, null, String.format(" limit %s", patchLimit), null, "map", null, null, "iotdb");
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("datasize: " + (linkedDataPoints == null ? "null": linkedDataPoints.size()));

            dataPoints = new ArrayList<>(newcomingData);
            dataPoints.addAll(linkedDataPoints);

            for(Map<String, Object> dataPoint : dataPoints) dataPoint.put(timelabel, dataPoint.get(timelabel).toString().replace("T", " "));

            // calc weights
            weights = new ArrayList<>();
            timeWeights = new ArrayList<>();
            valueWeights = new ArrayList<>();
            valuePresum = new ArrayList<>();
            buckets = new LinkedList<>();

            maxTimeWeight = 0;
            lastTimestamp = (Timestamp.valueOf(dataPoints.get(0).get(timelabel).toString().replace("T", " ").replace("Z", ""))).getTime();
            for(Map<String, Object> point : dataPoints){
                Date t = (Timestamp.valueOf(point.get(timelabel).toString().replace("T", " ").replace("Z", "")));
                Long weight = Math.abs(t.getTime() - lastTimestamp);
                timeWeights.add(weight);
                lastTimestamp = t.getTime();
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


            // sample complete, persist to iotdb
            if(sampleDataPoints == null) {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }

            // 删除上次的最后一桶
            try {
                String deletesql = String.format("delete from %s.%s.%s where time>=%s", database, tableName, columns, latestTime);
                System.out.println(deletesql);
                statement.execute(deletesql);
            }catch (IoTDBSQLException e){
                System.out.println(e.getMessage());
            } catch (SQLException e) {
                e.printStackTrace();
            }

            // 写入新一批样本
            for(Map<String, Object> map : sampleDataPoints){
                try {
                    statement.addBatch(String.format(batchInsertFormat, database, tableName, columns, map.get("time").toString().substring(0,19), map.get(label)));
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            try {
                statement.executeBatch();
                statement.clearBatch();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            // 单独处理最新桶
            newcomingData = buckets.get(buckets.size()-1).getDataPoints();
            latestTime = newcomingData.get(0).get("time").toString().substring(0,19);

            // 生命在于留白
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
