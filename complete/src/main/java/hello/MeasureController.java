package hello;

import java.util.*;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.*;
import org.apache.iotdb.jdbc.IoTDBSQLException;

@RestController
public class MeasureController {
    @RequestMapping("/measure")
    public Map<String, Object> dataPoints(
            @RequestParam(value="url", defaultValue = "jdbc:iotdb://127.0.0.1:6667/") String url,
            @RequestParam(value="username", defaultValue = "root") String username,
            @RequestParam(value="password", defaultValue = "root") String password,
            @RequestParam(value="database") String database,
            @RequestParam(value="timeseries") String timeseries,
            @RequestParam(value="columns") String columns,
            @RequestParam(value="starttime", required = false) String starttime,
            @RequestParam(value="endtime", required = false) String endtime,
            @RequestParam(value="conditions", required = false) String conditions,
            @RequestParam(value="query", required = false) String query,
            @RequestParam(value="format", defaultValue = "map") String format,
            @RequestParam(value="ip", required = false) String ip,
            @RequestParam(value="port", required = false) String port,
            @RequestParam(value="dbtype", defaultValue = "iotdb") String dbtype,
            @RequestParam(value="amount", required = false) Integer amount,
            @RequestParam(value="method", defaultValue = "iotdb") String method,
            @RequestParam(value="percent", defaultValue = "99995") Long percent,
            @RequestParam(value="alpha", defaultValue = "1") Double alpha
    ) throws SQLException {
        url = url.replace("\"", "");
        username = username.replace("\"", "");
        password = password.replace("\"", "");
        database = database.replace("\"", "");
        timeseries = timeseries.replace("\"", "");
        columns = columns.replace("\"", "");
        starttime = starttime == null ? null : starttime.replace("\"", "");
        endtime = endtime == null ? null :endtime.replace("\"", "");
        conditions = conditions == null ? null : conditions.replace("\"", "");
        format = format.replace("\"", "");
        dbtype = dbtype.replace("\"", "");
        ip = ip == null ? null : ip.replace("\"", "");
        port = port == null ? null : port.replace("\"", "");
        query = query == null ? null : query.replace("\"", "");
        method = method == null ? null : method.replace("\"", "");

        return null;
    }

    public static void main(String[] args) throws Exception {

        String url = "jdbc:iotdb://101.6.15.211:6667/";
        String username = "root";
        String password = "root";
        String database = "root.mxw2";
        String timeseries = "s2";
        String columns = "d3";
        String starttime = "3918-01-01 00:00:00";
        String endtime = "3918-01-10 00:00:00";
        String conditions = null;
        String format = "map";
        String dbtype = "iotdb";
        String ip = null;
        String port = null;
        String query = null;
        Long amount = 500L;
        Long percent = 99995L;
        Double alpha = 1.0;

        String iotdblabel = database + "." + timeseries + "." +columns;
        System.out.println(iotdblabel);
        String sampleLabel = database + "." + "L1.M9c840b71"+ "." +columns;
        System.out.println(sampleLabel);
        String label = dbtype.equals("iotdb") ? iotdblabel : columns;



        List<Map<String, Object>> originalData = new DataPointController().dataPoints(
                url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, "map", ip, port, dbtype);
        Double originalMean = calculateMean(originalData, label);
        Double originalStd = calculateStd(originalData, label, originalMean);
        Double originalRange = calculateRange(originalData, label);
        System.out.println(originalData.size());
//
//        List<Map<String, Object>> m4sampleData = new M4SampleController().dataPoints(
//                url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, "map", ip, port, amount, dbtype);
//        Double m4sampleMean = calculateMean(m4sampleData, label);
//        Double m4sampleStd = calculateStd(m4sampleData, label, m4sampleMean);
//        Double m4sampleRange = calculateRange(m4sampleData, label);
//
//
//        List<Map<String, Object>> gradsampleData = new GradSampleController().dataPoints(
//                url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, 50, 4, "map", ip, port, amount, dbtype);
//        Double gradsampleMean = calculateMean(gradsampleData, label);
//        Double gradsampleStd = calculateStd(gradsampleData, label, gradsampleMean);
//        Double gradsampleRange = calculateRange(gradsampleData, label);
//
//
        List<Map<String, Object>> bucketsampleData = new BucketSampleController().dataPoints(
                url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, 50, 4, "map", ip, port, Math.toIntExact(amount), dbtype, percent, alpha);
        Double bucketsampleMean = calculateMean(bucketsampleData, label);
        Double bucketsampleStd = calculateStd(bucketsampleData, label, bucketsampleMean);
        Double bucketsampleRange = calculateRange(bucketsampleData, label);
        System.out.println(bucketsampleData.size());

        List<Map<String, Object>> levelsampleData = new PublishController().publish(
                url, username, password, database, timeseries, columns, starttime, endtime, amount, dbtype);
        System.out.println(levelsampleData.size());
        Double levelsampleMean = calculateMean(levelsampleData, sampleLabel);
        System.out.println(levelsampleMean);
        Double levelsampleStd = calculateStd(levelsampleData, sampleLabel, levelsampleMean);
        System.out.println(levelsampleStd);
        Double levelsampleRange = calculateRange(levelsampleData, sampleLabel);
        System.out.println(levelsampleRange);
//
//        System.out.println("origin size:" + originalData.size());
//        System.out.println("amount: " + amount);
////        System.out.println(String.format("mean: %s, %s, %s, %s", originalMean, m4sampleMean, gradsampleMean, bucketsampleMean));
////        System.out.println(String.format("std: %s, %s, %s, %s", originalStd, m4sampleStd, gradsampleStd, bucketsampleStd));
////        System.out.println(String.format("range: %s, %s, %s, %s", originalRange, m4sampleRange, gradsampleRange, bucketsampleRange));
//
        Double max = null;
        Double min = null;
        for (Map<String, Object> map : originalData){
            if(map.containsKey(label)){
                Double v = (int)map.get(label) + 0.0;
                max = max == null ? v : Math.max(max, v);
                min = min == null ? v : Math.min(min, v);
            }
        }
////
        double[] originalDistribution = calculateDistribution(originalData, label, max, min);
//        double[] m4sampleDistribution = calculateDistribution(m4sampleData, label, max, min);
//        double[] gradsampleDistribution = calculateDistribution(gradsampleData, label, max, min);
        double[] bucketsampleDistribution = calculateDistribution(bucketsampleData, label, max, min);
        double[] levelsampleDistribution = calculateDistribution(levelsampleData, sampleLabel, max, min);
//
//        System.out.println("m4sampleDistributionError: " + calculateDistributionError(originalDistribution, m4sampleDistribution));
//        System.out.println("gradsampleDistributionError: " +  + calculateDistributionError(originalDistribution, gradsampleDistribution));
//        System.out.println("bucketsampleDistributionError: " +  + calculateDistributionError(originalDistribution, bucketsampleDistribution));

        System.out.println(String.format("mean: %s, %s, %s", originalMean, bucketsampleMean, levelsampleMean));
        System.out.println(String.format("std: %s, %s, %s", originalStd, bucketsampleStd, levelsampleStd));
        System.out.println(String.format("range: %s, %s, %s", originalRange, bucketsampleRange, levelsampleRange));
        System.out.println(String.format("distribution: 0, %s, %s, %s", calculateDistributionError(originalDistribution, bucketsampleDistribution), calculateDistributionError(originalDistribution, levelsampleDistribution), calculateDistributionError(bucketsampleDistribution, levelsampleDistribution)));

        for(int i = 0; i < originalDistribution.length; i++){
            System.out.println(originalDistribution[i] + ", " + bucketsampleDistribution[i] + ", " + levelsampleDistribution[i]);
        }
    }

    static double calculateMean(List<Map<String, Object>> data, String valueLabel){
        double sum = 0.0;
        int count = 0;
        for (Map<String, Object> map : data){
            if(map.containsKey(valueLabel)){
                sum += (int)map.get(valueLabel) + 0.0;
                count += 1;
            }
        }
        return sum / count;
    };

    static Double calculateStd(List<Map<String, Object>> data, String valueLabel, Double mean){
        double sum = 0.0;
        int count = 0;
        for (Map<String, Object> map : data){
            if(map.containsKey(valueLabel)){
                Double e = ((int)map.get(valueLabel) + 0.0 - mean);
                sum += e * e;
                count += 1;
            }
        }
        return Math.sqrt(sum / count);
    };

    static Double calculateRange(List<Map<String, Object>> data, String valueLabel){
        Double max = null;
        Double min = null;
        for (Map<String, Object> map : data){
            if(map.containsKey(valueLabel)){
                Double v = (int)map.get(valueLabel) + 0.0;
                max = max == null ? v : Math.max(max, v);
                min = min == null ? v : Math.min(min, v);
            }
        }
        return max == null ? 0 : max - min;
    };

    static double[] calculateDistribution(List<Map<String, Object>> data, String valueLabel, double max, double min){
        double range = max - min;
        double[] distribution = new double[11];
        int count = 0;
        for (Map<String, Object> map : data){
            if(map.containsKey(valueLabel)){
                count++;
                Double v = (int)map.get(valueLabel) + 0.0;
                int index = (int)Math.round((v - min) / range * 10) ;
                distribution[index] += 1;
            }
        }
        for(int i = 0; i < 11; i++) {
            distribution[i] /= count;
        }
        return distribution;
    }

    static double calculateDistributionError(double[] distribution1, double[] distribution2){
        double sum = 0;
        for(int i = 0; i < 11; i++){
            double v = distribution1[i] - distribution2[i];
            sum += v * v;
        }
        return Math.sqrt(sum);
    }
}
