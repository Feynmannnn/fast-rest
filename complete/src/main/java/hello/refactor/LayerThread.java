package hello.refactor;

import com.alibaba.fastjson.JSONObject;
import hello.ErrorController;
import hello.refactor.obj.Bucket;
import hello.refactor.source.PGConnection;
import org.springframework.util.DigestUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LayerThread extends Thread{

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
    Integer ratio;
    Long interval = 300L;
    String subId;
    String latestTime = "";
    Integer level = 0;
    List<Map<String, Object>> newcomingData = new LinkedList<>();
    String sample;
    String dbtype;
    Double percent;
    Double alpha;
    long bucketSum;
    final long setuptime;
    long throughput;
    long batchlimit;
    Lock lock;
    Condition lockCondition;

    LayerThread(String url, String username, String password, String database, String timeseries, String columns, String starttime, String TYPE, Integer ratio, String subId, Integer level, String sample, String dbtype, Double percent, Double alpha, Long batchlimit, Condition lockCondition){
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
        this.timeseries = timeseries;
        this.columns = columns;
        this.starttime = starttime;
        this.TYPE = TYPE;
        this.ratio = ratio;
        this.percent = percent;
        this.alpha = alpha;
        this.subId = subId;
        this.level = level;
        this.sample = sample;
        this.dbtype = dbtype;
        for(int i = 0; i < level; i++){
            interval *= 50;
        }
        this.setuptime = System.currentTimeMillis();
        this.throughput = 0L;
        this.batchlimit = batchlimit;
        this.lockCondition = lockCondition;
    }

    @Override
    public void run() {

        long kickOffThreshold = 10000L;
        boolean hadKickOff = false;
        Lock lock = new ReentrantLock();
        Condition newCondition = lock.newCondition();


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
                innerUrl + pgDatabase.toLowerCase(),
                innerUserName,
                innerPassword
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
        String iotdbLabel = database + "." + timeseries + "." +columns;
        String weightLabel = dbtype.equals("iotdb") ? database + "." + timeseries + ".weight" : "weight";
        System.out.println(weightLabel);
        String label = dbtype.equals("iotdb") ? iotdbLabel : columns.toLowerCase();
        String timelabel = "time";

        // fetch first patch

        List<Map<String, Object>> linkedDataPoints = null;
        try {
            linkedDataPoints = DataController._dataPoints(
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
                    level > 0 ? "pg" : dbtype);
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
        List<Double> timeWeights = new ArrayList<>();
        List<Double> valueWeights = new ArrayList<>();
        List<Double> grads = new ArrayList<>();
        List<Bucket> buckets = new LinkedList<>();
        long maxTimeWeight = 0;
        Double valueSum = 0.0;
        double maxValueWeight = 0.0;

        if(level == 0) {
            Long firstts = (Timestamp.valueOf(dataPoints.get(0).get("time").toString().replace("T", " ").replace("Z", ""))).getTime();
            Long lastts = (Timestamp.valueOf(dataPoints.get(dataPoints.size() - 1).get("time").toString().replace("T", " ").replace("Z", ""))).getTime();
            Long timestampRange = lastts - firstts;

            long lastTimestamp = (Timestamp.valueOf(dataPoints.get(0).get(timelabel).toString().replace("T", " ").replace("Z", ""))).getTime();
            for (Map<String, Object> point : dataPoints) {
                Date t = (Timestamp.valueOf(point.get(timelabel).toString().replace("T", " ").replace("Z", "")));
                Double weight = (t.getTime() - lastTimestamp) + 0.0;
                timeWeights.add(weight);
                lastTimestamp = t.getTime();
            }

            System.out.println("percent" + percent);

            Object lastValue = dataPoints.get(0).get(label);
            Double maxValue, minValue;
            if (lastValue instanceof Double) {
                maxValue = ((Double) lastValue);
                minValue = ((Double) lastValue);
            } else if (lastValue instanceof Long) {
                maxValue = (((Long) lastValue).doubleValue());
                minValue = (((Long) lastValue).doubleValue());
            } else if (lastValue instanceof Integer) {
                maxValue = (((Integer) lastValue).doubleValue());
                minValue = (((Integer) lastValue).doubleValue());
            } else {
                maxValue = ((Double) lastValue);
                minValue = ((Double) lastValue);
            }

            for (Map<String, Object> point : dataPoints) {
                Object value = point.get(label);
                double v;
                if (value instanceof Double) {
                    v = ((Double) value - (Double) lastValue);
                    maxValue = Math.max(maxValue, (Double) value);
                    minValue = Math.min(minValue, (Double) value);
                } else if (value instanceof Long) {
                    v = (((Long) value).doubleValue() - ((Long) lastValue).doubleValue());
                    maxValue = Math.max(maxValue, ((Long) value).doubleValue());
                    minValue = Math.min(minValue, ((Long) value).doubleValue());
                } else if (value instanceof Integer) {
                    v = (((Integer) value).doubleValue() - ((Integer) lastValue).doubleValue());
                    maxValue = Math.max(maxValue, ((Integer) value).doubleValue());
                    minValue = Math.min(minValue, ((Integer) value).doubleValue());
                } else {
                    System.out.println("label" + label);
                    v = ((Double) value - (Double) lastValue);
                    maxValue = Math.max(maxValue, (Double) value);
                    minValue = Math.min(minValue, (Double) value);
                }
                double valueWeight = (v);
                valueWeights.add(valueWeight);
                lastValue = value;
            }
            double valueRange = maxValue - minValue;

            double grad = 0.0;
            for (int i = 1; i < dataPoints.size(); i++) {
                if (timeWeights.get(i) >= percent || valueWeights.get(i) >= alpha) grad = Double.POSITIVE_INFINITY;
                else grad = Math.atan(valueWeights.get(i) / timeWeights.get(i));
                grads.add(grad);
            }
            grads.add(grad);

            Double maxWeight = 0.0;
            weights.add(0.0);
            for (int i = 1; i < dataPoints.size() - 1; i++) {
                if (Double.isInfinite(grads.get(i)) || Double.isInfinite(grads.get(i - 1))) {
                    weights.add(-1.0);
                } else {
                    double t1 = timeWeights.get(i) / percent * 25;
                    double t2 = timeWeights.get(i + 1) / percent * 25;
                    double v1 = valueWeights.get(i) / alpha;
                    double v2 = valueWeights.get(i + 1) / alpha;
                    double AB = Math.sqrt(t1 * t1 + v1 * v1);
                    double BC = Math.sqrt(t2 * t2 + v2 * v2);
                    double w = (AB + BC);
                    maxWeight = Math.max(w, maxWeight);
                    weights.add(w);
                }
            }
            weights.add(0.0);

            for (int i = 0; i < weights.size(); i++) {
                if (weights.get(i) > 0) weights.set(i, weights.get(i) * 100 / maxWeight);
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

        System.out.println(dataPoints.size());
        System.out.println(weights.size());

        // 二分查找
        int n = dataPoints.size() / 4 / ratio;
        long lo = 0, hi = weights.size() * 100;
        while (lo < hi){
            long mid = lo + (hi - lo >> 1);
            int count = 0;
            double sum = 0;
            for (double weight : weights) {
                if(weight < 0){
                    if(sum > 0) count++;
                    sum = 0;
                }
                else if (sum + weight > mid) {
                    sum = weight;
                    if (++count > n) break;
                }
                else sum += weight;
            }
            count++;
            if(count >= n) lo = mid + 1;
            else hi = mid;
        }
        long bucketSum = lo;
        System.out.println("bucketSum" + bucketSum);
        double sum = 0;
        int lastIndex = 0;

        for(int i = 0; i < weights.size(); i++){
            double weight = weights.get(i);
            if(weight < 0){
                if(sum > 0) {
                    buckets.add(new Bucket(dataPoints.subList(lastIndex, i)));
                    buckets.add(new Bucket(dataPoints.subList(i, i+1)));
                    lastIndex = i+1;
                    sum = 0;
                }
                else{
                    buckets.add(new Bucket(dataPoints.subList(i, i+1)));
                    lastIndex = i+1;
                    sum = 0;
                }
            }
            if(sum + weight > bucketSum){
                buckets.add(new Bucket(dataPoints.subList(lastIndex, i)));
                lastIndex = i;
                sum = weight;
            }
            else sum += weight;
        }
        buckets.add(new Bucket(dataPoints.subList(lastIndex, dataPoints.size())));

        for(int i = 0; i < buckets.size(); i++){
            for(Map<String, Object> p : buckets.get(i).getDataPoints()){
                if((Double)p.get("weight") < 0){
                    p.put("bucket", -1);
                }
                else p.put("bucket", i);
            }
        }

        System.out.println(buckets.size());

        // 进行桶内采样
        List<Map<String, Object>> sampleDataPoints = new LinkedList<>();
        long st = System.currentTimeMillis();
        System.out.println("bucket sample started");

        for(Bucket bucket : buckets){
            List<Map<String, Object>> datapoints = bucket.getDataPoints();
            if(datapoints.size() <= 4){
                sampleDataPoints.addAll(datapoints);
                continue;
            }
            sampleDataPoints.add(datapoints.get(0));
            Map<String, Object> maxi = datapoints.get(0);
            Map<String, Object> mini = datapoints.get(0);
            for(int i = 1; i < datapoints.size()-1; i++){
                Map<String, Object> candi = datapoints.get(i);
                Object value = candi.get(label);
                if(value instanceof Double) maxi = (Double) value >= (Double)maxi.get(label) ? candi : maxi;
                else if(value instanceof Integer) maxi = (Integer) value >= (Integer)maxi.get(label) ? candi : maxi;
                else if(value instanceof Long) maxi = (Long) value >= (Long)maxi.get(label) ? candi : maxi;
                else maxi = (Double) value >= (Double)maxi.get(label) ? candi : maxi;

                if(value instanceof Double) mini = (Double) value <= (Double)mini.get(label) ? candi : mini;
                else if(value instanceof Integer) mini = (Integer) value <= (Integer)mini.get(label) ? candi : mini;
                else if(value instanceof Long) mini = (Long) value <= (Long)mini.get(label) ? candi : mini;
                else mini = (Double) value <= (Double)mini.get(label) ? candi : mini;
            }
            sampleDataPoints.add(maxi);
            sampleDataPoints.add(mini);
            sampleDataPoints.add(datapoints.get(datapoints.size()-1));
        }
        System.out.println("bucket sample used time: " + (System.currentTimeMillis() - st) + "ms");

        // 删除上次的最后一桶
        String deletesql = String.format("delete from %s where time>='%s'", tableName, latestTime);
        System.out.println(deletesql);
        pgtool.queryUpdate(connection, deletesql);

        throughput += dataPoints.size();
        long usedtime = System.currentTimeMillis() - setuptime;
        System.out.println(String.format("throughput:%d, used time: %d, average:%d", throughput, usedtime, throughput / usedtime * 1000));

        // sample complete, persist to iotdb
        String batchInsertFormat = "insert into %s (time, weight, error, area, %s) values ('%s+08', %s, %s, %s, %s);";

        Collections.sort(sampleDataPoints, sampleComparator);

        ErrorController.lineError(dataPoints, sampleDataPoints, label);

        List<String> sqls = new LinkedList<>();
        for(Map<String, Object> map : sampleDataPoints) {
            String sql = String.format(batchInsertFormat, tableName, columns, map.get("time").toString().substring(0,19), map.get("weight").toString(), map.get("error").toString(), map.get("area").toString(), map.get(label));
//            System.out.println(map.get("time"));
//            System.out.println(map.get("time").toString().substring(0,19));
//            System.out.println(sql);
            sqls.add(sql);
        }
        StringBuilder sb = new StringBuilder();
        for(String sql : sqls) sb.append(sql);
        String bigSql = sb.toString();
//        System.out.println(bigSql);
        pgtool.queryUpdate(connection, bigSql);


        // process the newest bucket
        newcomingData = buckets.get(buckets.size()-1).getDataPoints();
        latestTime = newcomingData.get(0).get("time").toString().substring(0,19);


        // 生命在于留白
//        try {
//            Thread.sleep(interval);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        // keep sampling data
        while(!exit){

            // wait for notify

//            System.out.println(latestTime);

            try {
                linkedDataPoints = DataController._dataPoints(
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
            if(level == 0){
                weights = new ArrayList<>();
                timeWeights = new ArrayList<>();
                valueWeights = new ArrayList<>();
                buckets = new LinkedList<>();

                grads = new ArrayList<>();
                buckets = new LinkedList<>();

                long firstts = (Timestamp.valueOf(dataPoints.get(0).get("time").toString().replace("T", " ").replace("Z", ""))).getTime();
                long lastts = (Timestamp.valueOf(dataPoints.get(dataPoints.size()-1).get("time").toString().replace("T", " ").replace("Z", ""))).getTime();
                long timestampRange = lastts - firstts;

                long lastTimestamp = (Timestamp.valueOf(dataPoints.get(0).get(timelabel).toString().replace("T", " ").replace("Z", ""))).getTime();
                for(Map<String, Object> point : dataPoints){
                    Date t = (Timestamp.valueOf(point.get(timelabel).toString().replace("T", " ").replace("Z", "")));
                    Double weight = (t.getTime() - lastTimestamp) + 0.0;
                    timeWeights.add(weight);
                    lastTimestamp = t.getTime();
                }

                System.out.println("percent" + percent);

                Object lastValue = dataPoints.get(0).get(label);
                Double maxValue, minValue;
                if(lastValue instanceof Double) {
                    maxValue = ((Double) lastValue);
                    minValue = ((Double) lastValue);
                }
                else if(lastValue instanceof Long) {
                    maxValue = (((Long) lastValue).doubleValue());
                    minValue = (((Long) lastValue).doubleValue());
                }
                else if(lastValue instanceof Integer) {
                    maxValue = (((Integer) lastValue).doubleValue());
                    minValue = (((Integer) lastValue).doubleValue());
                }
                else {
                    maxValue = ((Double) lastValue);
                    minValue = ((Double) lastValue);
                }

                for(Map<String, Object> point : dataPoints){
                    Object value = point.get(label);
                    double v;
                    if(value instanceof Double) {
                        v = ((Double) value - (Double) lastValue);
                        maxValue = Math.max(maxValue, (Double) value);
                        minValue = Math.min(minValue, (Double) value);
                    }
                    else if(value instanceof Long) {
                        v = (((Long) value).doubleValue() - ((Long) lastValue).doubleValue());
                        maxValue = Math.max(maxValue, ((Long) value).doubleValue());
                        minValue = Math.min(minValue, ((Long) value).doubleValue());
                    }
                    else if(value instanceof Integer) {
                        v = (((Integer) value).doubleValue() - ((Integer) lastValue).doubleValue());
                        maxValue = Math.max(maxValue, ((Integer) value).doubleValue());
                        minValue = Math.min(minValue, ((Integer) value).doubleValue());
                    }
                    else {
                        System.out.println("label" + label);
                        v = ((Double) value - (Double) lastValue);
                        maxValue = Math.max(maxValue, (Double) value);
                        minValue = Math.min(minValue, (Double) value);
                    }
                    double valueWeight = (v);
                    valueWeights.add(valueWeight);
                    lastValue = value;
                }
                double valueRange = maxValue - minValue;

                double grad = 0.0;
                for(int i = 1; i < dataPoints.size(); i++){
                    if(timeWeights.get(i) >= percent || valueWeights.get(i) >= alpha) grad = Double.POSITIVE_INFINITY;
                    else grad = Math.atan(valueWeights.get(i) / timeWeights.get(i));
                    grads.add(grad);
                }
                grads.add(grad);

                double maxWeight = 0.0;
                weights.add(0.0);
                for(int i = 1; i < dataPoints.size()-1; i++) {
                    if(Double.isInfinite(grads.get(i)) || Double.isInfinite(grads.get(i-1))){
                        weights.add(-1.0);
                    }
                    else{
                        double t1 = timeWeights.get(i) / timestampRange * 5;
                        double t2 = timeWeights.get(i+1) / timestampRange * 5;
                        double v1 = valueWeights.get(i) / valueRange;
                        double v2 = valueWeights.get(i+1) / valueRange;
                        double AB = Math.sqrt(t1 * t1 + v1 * v1);
                        double BC = Math.sqrt(t2 * t2 + v2 * v2);
                        double w = (AB + BC);
                        maxWeight = Math.max(w, maxWeight);
                        weights.add(w);
                    }
                }
                weights.add(0.0);

                for (int i = 0; i < weights.size(); i++){
                    if(weights.get(i) > 0) weights.set(i, weights.get(i) * 100 / maxWeight);
                    dataPoints.get(i).put("weight", weights.get(i));
                }

                System.out.println(dataPoints.size());
                System.out.println(weights.size());
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
                if(datapoints.size() <= 4){
                    sampleDataPoints.addAll(datapoints);
                    continue;
                }
                sampleDataPoints.add(datapoints.get(0));
                Map<String, Object> maxi = datapoints.get(0);
                Map<String, Object> mini = datapoints.get(0);
                for(int i = 1; i < datapoints.size()-1; i++){
                    Map<String, Object> candi = datapoints.get(i);
                    Object value = candi.get(label);
                    if(value instanceof Double) maxi = (Double) value >= (Double)maxi.get(label) ? candi : maxi;
                    else if(value instanceof Integer) maxi = (Integer) value >= (Integer)maxi.get(label) ? candi : maxi;
                    else if(value instanceof Long) maxi = (Long) value >= (Long)maxi.get(label) ? candi : maxi;
                    else maxi = (Double) value >= (Double)maxi.get(label) ? candi : maxi;

                    if(value instanceof Double) mini = (Double) value <= (Double)mini.get(label) ? candi : mini;
                    else if(value instanceof Integer) mini = (Integer) value <= (Integer)mini.get(label) ? candi : mini;
                    else if(value instanceof Long) mini = (Long) value <= (Long)mini.get(label) ? candi : mini;
                    else mini = (Double) value <= (Double)mini.get(label) ? candi : mini;
                }
                sampleDataPoints.add(maxi);
                sampleDataPoints.add(mini);
                sampleDataPoints.add(datapoints.get(datapoints.size()-1));
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

            ErrorController.lineError(dataPoints, sampleDataPoints, label);

            sqls = new LinkedList<>();
            for(Map<String, Object> map : sampleDataPoints){
                sqls.add(String.format(batchInsertFormat, tableName, columns, map.get("time").toString().substring(0,19), map.get("weight"), map.get("error"), map.get("area"), map.get(label)));
            }
            sb = new StringBuilder();
            for(String sql : sqls) sb.append(sql);
            bigSql = sb.toString();
//            System.out.println(bigSql);
            pgtool.queryUpdate(connection, bigSql);

            if(throughput > kickOffThreshold){
                if(hadKickOff){
                    // notify the next level
                    System.out.println("throughput" + throughput);
                    System.out.println("signal the " + (level + 1) + "level to continue;");
                    newCondition.signal();
                }
                else {
                    hadKickOff = true;
                    // kick off the next level sample
                    String Identifier = String.format("%s,%s,%s,%s,%s", url, database, tableName, columns, salt);
                    String newSubId = DigestUtils.md5DigestAsHex(Identifier.getBytes()).substring(0,8);
                    System.out.println(newSubId);
                    LayerThread pgsubscribeThread = new LayerThread(url, username, password, database, tableName, columns, starttime, TYPE, ratio, newSubId, level+1, sample, "pg", percent, alpha, batchlimit, newCondition);
                    pgsubscribeThread.start();
                }
                throughput = 0;
            }

            // sample complete, persist to iotdb
            if(dataPoints.size() < patchLimit) {
//                exit = true;
//                System.out.println("Level" + level + " catch up!<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
                if(level > 0){
                    try {
                        System.out.println("lock the level" + level + " <<<<<<<<<<<<<!!!!!!!!");
                        lockCondition.await();
                        System.out.println("notify the level" + level + " <<<<<<<<<<<<<!!!!!!!!");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            if(exit) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            // 单独处理最新桶
            newcomingData = buckets.get(buckets.size()-1).getDataPoints();
            latestTime = newcomingData.get(0).get("time").toString().substring(0,19);

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
