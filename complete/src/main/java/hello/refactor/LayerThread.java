package hello.refactor;

import com.alibaba.fastjson.JSONObject;
import hello.ErrorController;
import hello.refactor.obj.Bucket;
import hello.refactor.source.PGConnection;
import hello.refactor.util.OutlierDetection;
import org.springframework.util.DigestUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class LayerThread extends Thread{

    // 根据时间戳对数据进行排序 （从TimescaleDB查询的数据结果存在时间的乱序问题）
    private static Comparator<Map<String, Object>> sampleComparator = new Comparator<Map<String, Object>>(){
        @Override
        public int compare(Map<String, Object> sampleDataPoint1, Map<String, Object> sampleDataPoint2){
            return sampleDataPoint1.get("time").toString().compareTo(sampleDataPoint2.get("time").toString());
        }
    };

    public volatile boolean exit = false;
    private static final String salt = "&%12345***&&%%$$#@1";
    private String url;
    private String username;
    private String password;
    private String database;
    private String timeseries;
    private String columns;
    private String starttime;
    private String endtime;
    private String TYPE;
    private Integer ratio;
    private String subId;
    private Integer level = 0;
    private String sample;
    private String dbtype;
    private Double percent;
    private Double alpha;
    private final long setuptime;
    private long throughput;
    private long batchlimit;
    private BlockingQueue<Map<String, Object>> dataQueue;
    private BlockingQueue<Map<String, Object>> sampleQueue;
    private Long bucketSum;

    LayerThread(String url, String username, String password, String database, String timeseries, String columns, String starttime, String endtime, String TYPE, Integer ratio, String subId, Integer level, String sample, String dbtype, Double percent, Double alpha, Long batchlimit, BlockingQueue<Map<String, Object>> dataQueue, Long bucketSum){
        this.url = url;
        this.username = username;
        this.password = password;
        this.database = database;
        this.timeseries = timeseries;
        this.columns = columns;
        this.starttime = starttime;
        this.endtime = endtime;
        this.TYPE = TYPE;
        this.ratio = ratio;
        this.percent = percent;
        this.alpha = alpha;
        this.subId = subId;
        this.level = level;
        this.sample = sample;
        this.dbtype = dbtype;
        this.batchlimit = batchlimit;
        this.dataQueue = dataQueue;
        this.sampleQueue = new LinkedBlockingQueue<>();
        this.bucketSum = bucketSum;
        
        this.setuptime = System.currentTimeMillis();
        this.throughput = 0L;
    }

    @Override
    public void run() {

        // batchlimit L0层级每次从数据库读取的数据量
        batchlimit = 100000;

        // kickOffSampling 触发新一轮采样的累计数据数目
        long kickOffSampling = 1000L;

        // 触发新一层级样本采样的数据数目
        long kickOffThreshold = kickOffSampling * ratio;

        //是否已经触发过下一层级采样线程
        boolean hadKickOff = false;

        // 本层级样本在TSDB中的数据表
        String pgDatabase = database.replace('.', '_');
        String tableName = "L" + level + "_M" + subId;

        // 最新桶内的最早时间戳，下一批次数据的查询起始时间
        String latestTime = starttime;

        // 读取config文件获取内置数据库信息
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

        if(level == 0){
            String createDatabaseSql = String.format("create database %s;", pgDatabase);
            System.out.println(createDatabaseSql);
            pgtool.queryUpdate(connection, createDatabaseSql);
        }
        // create database if not exist


        pgtool = new PGConnection(
                innerUrl + pgDatabase.toLowerCase(),
                innerUserName,
                innerPassword
        );
        connection = pgtool.getConn();

        if(level == 0){
            String extentionsql = "CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;";
            pgtool.queryUpdate(connection, extentionsql);
        }

        // create table
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

        // 获取第一批数据，目标为计算BucketSum与时间数值的离群值阈值
        List<Map<String, Object>> linkedDataPoints = null;
        try {
            if(level > 0){
                linkedDataPoints = new LinkedList<>();
                long k = Math.min(dataQueue.size(), batchlimit);
                System.out.println("k = " + k);
                for(int i = 0; i < k; i++){
                    linkedDataPoints.add(dataQueue.poll());
                }
            }
            else {// level == 0
                linkedDataPoints = DataController._dataPoints(
                    url,
                    username,
                    password,
                    database,
                    timeseries,
                    columns,
                    latestTime,
                    null,
                    String.format(" limit %s", batchlimit),
                    null,
                    "map",
                    null,
                    null,
                    dbtype);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("datasize: " + (linkedDataPoints == null ? "null": linkedDataPoints.size()));
        List<Map<String, Object>> dataPoints = new ArrayList<>(linkedDataPoints);
        System.out.println("first data point:" + dataPoints.get(0).get("time").toString());
        System.out.println("last data point:" + dataPoints.get(dataPoints.size() - 1).get("time").toString());

        // time format change for iotdb
        if(dbtype.equals("iotdb")) for(Map<String, Object> dataPoint : dataPoints) dataPoint.put(timelabel, dataPoint.get(timelabel).toString().replace("T", " "));

        // first weights calc
        // TODO: js version of datatype compatible
        List<Double> weights = new ArrayList<>();
        List<Double> timeWeights = new ArrayList<>();
        List<Double> valueWeights = new ArrayList<>();
        List<Double> grads = new ArrayList<>();
        List<Bucket> buckets = new LinkedList<>();
//        long maxTimeWeight = 0;
//        Double valueSum = 0.0;
//        double maxValueWeight = 0.0;

        boolean percentIsNull = true;
        boolean alphaIsNull = true;
        System.out.println("level " + level);
        System.out.println(percentIsNull);
        System.out.println(alphaIsNull);

        if(level == 0) {
//            Long firstts = (Timestamp.valueOf(dataPoints.get(0).get("time").toString().replace("T", " ").replace("Z", ""))).getTime();
//            Long lastts = (Timestamp.valueOf(dataPoints.get(dataPoints.size() - 1).get("time").toString().replace("T", " ").replace("Z", ""))).getTime();
//            Long timestampRange = lastts - firstts;

            long lastTimestamp = (Timestamp.valueOf(dataPoints.get(0).get(timelabel).toString().replace("T", " ").replace("Z", ""))).getTime();
            for (Map<String, Object> point : dataPoints) {
                Date t = (Timestamp.valueOf(point.get(timelabel).toString().replace("T", " ").replace("Z", "")));
                Double weight = (t.getTime() - lastTimestamp) + 0.0;
                timeWeights.add(weight);
                lastTimestamp = t.getTime();
            }

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

            if(percentIsNull){
                int timeOutlierNum = OutlierDetection.zscoreOutlierNum(timeWeights, 3);
                System.out.println("timeOutlierNum" + timeOutlierNum);
                Double[] timeWeightStat = timeWeights.toArray(new Double[0]);
                Arrays.sort(timeWeightStat);
                percent = timeWeightStat[timeWeights.size() - (timeOutlierNum > 0 ? timeOutlierNum : 1)];
                if(percent <= 0) percent = timeWeightStat[timeWeights.size() * 9999 / 10000];
            }

            if(alphaIsNull){
                int valueOutlierNum = OutlierDetection.zscoreOutlierNum(valueWeights, 3);
                System.out.println("valueOutlierNum" + valueOutlierNum);
                Double[] valueWeightStat = valueWeights.toArray(new Double[0]);
                Arrays.sort(valueWeightStat);
                alpha = valueWeightStat[valueWeights.size() - (valueOutlierNum > 0 ? valueOutlierNum : 1)];
                if(alpha <= 0) alpha = valueWeightStat[valueWeights.size() * 9999 / 10000];
            }

            System.out.println(percent);
            System.out.println(alpha);

            double grad = 0.0;
            for (int i = 1; i < dataPoints.size(); i++) {
                System.out.println(timeWeights.get(i) + "," + valueWeights.get(i));
                if (timeWeights.get(i) > percent || valueWeights.get(i) > alpha) grad = Double.POSITIVE_INFINITY;
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
                    double t1 = timeWeights.get(i) * 100  / percent;
                    double t2 = timeWeights.get(i + 1) * 100  / percent;
                    double v1 = valueWeights.get(i) * 100  / alpha;
                    double v2 = valueWeights.get(i + 1) * 100  / alpha;
                    double AB = Math.sqrt(t1 * t1 + v1 * v1);
                    double BC = Math.sqrt(t2 * t2 + v2 * v2);
                    double w = (AB + BC);
                    if(Double.isNaN(w)) w = 0;
                    maxWeight = Math.max(w, maxWeight);
                    weights.add(w);
                }
            }
            weights.add(0.0);

            for (int i = 0; i < weights.size(); i++) {
//                if (weights.get(i) > 0) weights.set(i, weights.get(i) * 100 / maxWeight);
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
        int outliers = 0;
        long lo = 0, hi = weights.size() * 100;
        while (lo < hi){
            long mid = lo + (hi - lo >> 1);
            int count = 0;
            double sum = 0;
            for (double weight : weights) {
                if(weight < 0){
                    outliers++;
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
        bucketSum = bucketSum == null ? lo : bucketSum;
        System.out.println("level " + level + " bucketSum:" + bucketSum);
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
            Set<Map<String, Object>> set = new HashSet<>();
            set.add(datapoints.get(0));
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
            set.add(maxi);
            set.add(mini);
            set.add(datapoints.get(datapoints.size()-1));
            sampleDataPoints.addAll(set);
        }
        System.out.println("bucket sample used time: " + (System.currentTimeMillis() - st) + "ms");
        System.out.println("first sample point:" + sampleDataPoints.get(0).get("time").toString());
        System.out.println("last sample point:" + sampleDataPoints.get(sampleDataPoints.size() - 1).get("time").toString());


        // 删除上次的最后一桶
        String deletesql = String.format("delete from %s where time>='%s'", tableName, latestTime);

        // sample complete, persist to iotdb
        String batchInsertFormat = "insert into %s (time, weight, error, area, %s) values ('%s', %s, %s, %s, %s);";

        List<String> sqls;
        StringBuilder sb;
        String bigSql;

        // 删除上次的最后一桶
        deletesql = String.format("delete from %s where time>='%s'", tableName, latestTime);
        System.out.println(deletesql);
        pgtool.queryUpdate(connection, deletesql);

        // 写入新一批样本
        sampleDataPoints.sort(sampleComparator);
        ErrorController.lineError(dataPoints, sampleDataPoints, label, level == 0);
        sqls = new LinkedList<>();
        for(Map<String, Object> map : sampleDataPoints){
            sqls.add(String.format(batchInsertFormat, tableName, columns, map.get("time").toString(), map.get("weight"), map.get("error"), map.get("area"), map.get(label)));
        }
        sb = new StringBuilder();
        for(String sql : sqls) {
            if(sql.toLowerCase().contains("nan")) System.out.println(sql);
            else sb.append(sql);
        }
        bigSql = sb.toString();
        pgtool.queryUpdate(connection, bigSql);

        // 更新sampleQueue给下一层级
        for(Map<String, Object> s : sampleDataPoints) s.put(columns.toLowerCase(), s.get(label));
        sampleQueue.addAll(sampleDataPoints);
        System.out.println("sampleQueue size:" + sampleQueue.size());

        latestTime = buckets.get(buckets.size() - 1).getDataPoints().get(0).get("time").toString();

        // 统计throughput
//        throughput += dataPoints.size();
//        usedtime = System.currentTimeMillis() - setuptime;
//        long throughtime = System.currentTimeMillis() - sts + 1L;
//        System.out.println(String.format("throughput:%d, used time: %d, average:%d", throughput, usedtime, dataPoints.size() * 1000 / throughtime));



        // keep sampling data
        while(!exit){

            long roundStartTime = System.currentTimeMillis();

            if (level > 0) {
                while (true){
                    if(dataQueue.size() >= kickOffSampling){
                        break;
                    }
                    else {
                        try {
                            Thread.sleep(100 * level);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            else {
                // level = 0
                if(dataPoints.size() < batchlimit) {
                    System.out.println("L0 catched up <<<<<<<<<<<<<<<!!!!!!!!!!!!!!");
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            try {
                if(level > 0){
                    linkedDataPoints = new LinkedList<>();
                    long k = Math.min(dataQueue.size(), batchlimit);
                    System.out.println("k = " + k);
                    for(int i = 0; i < k; i++){
                        linkedDataPoints.add(dataQueue.poll());
                    }
                }
                else {
                    // level = 0
                    linkedDataPoints = DataController._dataPoints(
                        url,
                        username,
                        password,
                        database,
                        timeseries,
                        columns,
                        latestTime,
                        null,
                        String.format(" limit %s", batchlimit),
                        null,
                        "map",
                        null,
                        null,
                        dbtype);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            dataPoints = new ArrayList<>(linkedDataPoints);

            for(Map<String, Object> dataPoint : dataPoints) dataPoint.put(timelabel, dataPoint.get(timelabel).toString().replace("T", " "));

            dataPoints.sort(sampleComparator);

            // calc weights
            {
                weights = new ArrayList<>();
                timeWeights = new ArrayList<>();
                valueWeights = new ArrayList<>();
                grads = new ArrayList<>();

                if(level == 0) {

                    long lastTimestamp = (Timestamp.valueOf(dataPoints.get(0).get(timelabel).toString().replace("T", " ").replace("Z", ""))).getTime();
                    for (Map<String, Object> point : dataPoints) {
                        Date t = (Timestamp.valueOf(point.get(timelabel).toString().replace("T", " ").replace("Z", "")));
                        Double weight = (t.getTime() - lastTimestamp) + 0.0;
                        timeWeights.add(weight);
                        lastTimestamp = t.getTime();
                    }

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
                        if (timeWeights.get(i) > percent || valueWeights.get(i) > alpha) grad = Double.POSITIVE_INFINITY;
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
                            double t1 = timeWeights.get(i) * 100 / percent;
                            double t2 = timeWeights.get(i + 1)  * 100 / percent;
                            double v1 = valueWeights.get(i) * 100  / alpha;
                            double v2 = valueWeights.get(i + 1) * 100  / alpha;
                            double AB = Math.sqrt(t1 * t1 + v1 * v1);
                            double BC = Math.sqrt(t2 * t2 + v2 * v2);
                            double w = (AB + BC);
                            if(Double.isNaN(w)) w = 0;
                            maxWeight = Math.max(w, maxWeight);
                            weights.add(w);
                        }
                    }
                    weights.add(0.0);

                    for (int i = 0; i < weights.size(); i++) {
//                        if (weights.get(i) > 0) weights.set(i, weights.get(i) * 100 / maxWeight);
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
            }

            // divide bucket
            buckets = new ArrayList<>();
            int bcount  = 0;
            {
                sum = 0;
                lastIndex = 0;
                Bucket b;

                for(int i = 0; i < weights.size(); i++){
                    double weight = weights.get(i);
                    if(weight < 0){
                        if(sum > 0) {
                            b = new Bucket(dataPoints.subList(lastIndex, i));
                            bcount += b.getDataPoints().size();
                            buckets.add(b);
                            b = new Bucket(dataPoints.subList(i, i+1));
                            bcount += b.getDataPoints().size();
                            buckets.add(b);
                            lastIndex = i+1;
                            sum = 0;
                        }
                        else{
                            b = new Bucket(dataPoints.subList(i, i+1));
                            bcount += b.getDataPoints().size();
                            buckets.add(b);
                            lastIndex = i+1;
                            sum = 0;
                        }
                    }
                    if(sum + weight > bucketSum){
                        b = new Bucket(dataPoints.subList(lastIndex, i));
                        bcount += b.getDataPoints().size();
                        buckets.add(b);
                        lastIndex = i;
                        sum = weight;
                    }
                    else sum += weight;
                }
                b = new Bucket(dataPoints.subList(lastIndex, dataPoints.size()));
                bcount += b.getDataPoints().size();
                buckets.add(b);
            }

            System.out.println("buckets size:" + buckets.size());

            // 桶内采样
            int count = 0;
            {
                sampleDataPoints = new LinkedList<>();
                st = System.currentTimeMillis();
                for (Bucket bucket : buckets) {
                    List<Map<String, Object>> datapoints = bucket.getDataPoints();
                    count += datapoints.size();
                    if (datapoints.size() <= 4) {
                        sampleDataPoints.addAll(datapoints);
                        continue;
                    }
                    sampleDataPoints.add(datapoints.get(0));
                    Map<String, Object> maxi = datapoints.get(0);
                    Map<String, Object> mini = datapoints.get(0);
                    for (int i = 1; i < datapoints.size() - 1; i++) {
                        Map<String, Object> candi = datapoints.get(i);
                        Object value = candi.get(label);
                        if (value instanceof Double) maxi = (Double) value >= (Double) maxi.get(label) ? candi : maxi;
                        else if (value instanceof Integer)
                            maxi = (Integer) value >= (Integer) maxi.get(label) ? candi : maxi;
                        else if (value instanceof Long) maxi = (Long) value >= (Long) maxi.get(label) ? candi : maxi;
                        else maxi = (Double) value >= (Double) maxi.get(label) ? candi : maxi;

                        if (value instanceof Double) mini = (Double) value <= (Double) mini.get(label) ? candi : mini;
                        else if (value instanceof Integer)
                            mini = (Integer) value <= (Integer) mini.get(label) ? candi : mini;
                        else if (value instanceof Long) mini = (Long) value <= (Long) mini.get(label) ? candi : mini;
                        else mini = (Double) value <= (Double) mini.get(label) ? candi : mini;
                    }
                    sampleDataPoints.add(maxi);
                    sampleDataPoints.add(mini);
                    sampleDataPoints.add(datapoints.get(datapoints.size() - 1));
                }
            }
            System.out.println("data points count:" + count);

            System.out.println(dataPoints.size());
            System.out.println(sampleDataPoints.size());

            // 删除上次的最后一桶内的样本
            deletesql = String.format("delete from %s where time>='%s'", tableName, latestTime);
            System.out.println(deletesql);
            pgtool.queryUpdate(connection, deletesql);

            // 写入新一批样本
            sampleDataPoints.sort(sampleComparator);
            ErrorController.lineError(dataPoints, sampleDataPoints, label, level == 0);
            sqls = new LinkedList<>();
            for(Map<String, Object> map : sampleDataPoints){
                sqls.add(String.format(batchInsertFormat, tableName, columns, map.get("time").toString(), map.get("weight"), map.get("error"), map.get("area"), map.get(label)));
            }
            sb = new StringBuilder();
            for(String sql : sqls) {
                if(sql.toLowerCase().contains("nan")) System.out.println(sql);
                else sb.append(sql);
            }
            bigSql = sb.toString();
            pgtool.queryUpdate(connection, bigSql);

            // 更新sampleQueue给下一层级
            for(Map<String, Object> s : sampleDataPoints) s.put(columns.toLowerCase(), s.get(label));
            sampleQueue.addAll(sampleDataPoints);

            // 统计throughput
            throughput += dataPoints.size();
            long usedtime = System.currentTimeMillis() - setuptime;
            long roundtime = System.currentTimeMillis() - roundStartTime + 1L;
            System.out.println(String.format("throughput:%d, used time: %d, average:%d", throughput, usedtime, dataPoints.size() * 1000 / roundtime));

            // 根据throughput触发下一层级
            if(!hadKickOff && throughput > kickOffThreshold){
                hadKickOff = true;
                String Identifier = String.format("%s,%s,%s,%s,%s", url, database, tableName, columns, salt);
                String newSubId = DigestUtils.md5DigestAsHex(Identifier.getBytes()).substring(0,8);
                System.out.println("kick off the level " + (level +1) + "<<<<<<!!!!!!!");
                System.out.println(newSubId);
                LayerThread pgsubscribeThread = new LayerThread(url, username, password, database, tableName, columns, starttime, endtime, TYPE, ratio, newSubId, level+1, sample, "pg", percent * ratio, alpha, batchlimit, sampleQueue, bucketSum * ratio / 2);
                pgsubscribeThread.start();
            }

            if(exit) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }

            // 单独处理最新桶，获得latestTime
            List<Map<String, Object>> newcomingData = buckets.get(buckets.size() - 1).getDataPoints();
            latestTime = newcomingData.get(0).get("time").toString();

            // 采样终止条件：latestTime大于截止日期
            if(endtime != null && latestTime.compareTo(endtime) >= 0){
                exit = true;
                System.out.println("L" + level + " catched up <<<<<<<<<<<<<<<!!!!!!!!!!!!!!");
            }

            System.out.println("buckets.size():" + buckets.size());
            System.out.println(String.format("L%d data size: %s, sample size: %s, ratio: %s", level, dataPoints.size(), sampleDataPoints.size(), dataPoints.size() / sampleDataPoints.size()));

            // 生命在于留白
            try {
                long interval = 300L;
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
