package hello;

import java.sql.Timestamp;
import java.util.*;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ErrorController {

    private static Comparator<Map<String, Object>> sampleComparator = new Comparator<Map<String, Object>>(){
        @Override
        public int compare(Map<String, Object> sampleDataPoint1, Map<String, Object> sampleDataPoint2){
            long t1 = (Timestamp.valueOf(sampleDataPoint1.get("time").toString())).getTime();
            long t2 = (Timestamp.valueOf(sampleDataPoint2.get("time").toString())).getTime();
            return Math.round(t1-t2);
        }
    };

    @RequestMapping("/lineerror")
    Double linearError(@RequestParam(value="url", defaultValue = "jdbc:iotdb://127.0.0.1:6667/") String url,
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
            @RequestParam(value="amount", required = false) Integer amount,
            @RequestParam(value="dbtype", defaultValue = "iotdb") String dbtype,
            @RequestParam(value="percent", defaultValue = "1") Double percent,
            @RequestParam(value="alpha", defaultValue = "1") Double alpha) throws Exception {
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

        String iotdblabel = database + "." + timeseries + "." +columns;
        String label = dbtype.equals("iotdb") ? iotdblabel : columns;

        List<Map<String, Object>> ldata = new DataPointController().dataPoints(url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, "map", ip, port, dbtype);
        List<Map<String, Object>> data = new ArrayList<>(ldata);
        List<Map<String, Object>> lsample = new M4SampleController().dataPoints(url, username, password, database, timeseries, columns, starttime, endtime, conditions, query, "map", ip, port, amount, dbtype, percent, alpha);
        List<Map<String, Object>> sample = new ArrayList<>(lsample);

        lineError(data, sample, label);

        double error= 0.0;
        double area = 0.0;

        for(int i = 0; i < sample.size(); i++){
//            System.out.println(i);
            error += (double)sample.get(i).get("error");
            area += (double)sample.get(i).get("area");
        }

        System.out.println(error);
        System.out.println(area);
        System.out.println(error / area);

        return error / area;
    }

    public static void lineError(List<Map<String, Object>> data, List<Map<String, Object>> sample, String label){

        if(sample.size() < 1) return;
        sample.get(0).put("error", 0.0);
        sample.get(0).put("area", 0.0);
        if(sample.size() == 1) return;

        int lastIndex = 1;
        int newIndex;
        for(int i = 1; i < sample.size(); i++){
            Long a0L = (Timestamp.valueOf(sample.get(i-1).get("time").toString())).getTime();
            double a0 = a0L.doubleValue();
            Long a1L = (Timestamp.valueOf(sample.get(i).get("time").toString())).getTime();
            double a1 = a1L.doubleValue();
            double b0, b1;
            Object value = sample.get(i-1).get(label);
            if(value instanceof Double) b0 = (Double) value;
            else if(value instanceof Integer) b0 = ((Integer) value).doubleValue();
            else if(value instanceof Long) b0 = ((Long) value).doubleValue();
            else b0 = (Double) value;

            value = sample.get(i).get(label);
            if(value instanceof Double) b1 = (Double) value;
            else if(value instanceof Integer) b1 = ((Integer) value).doubleValue();
            else if(value instanceof Long) b1 = ((Long) value).doubleValue();
            else b1 = (Double) value;

            for(newIndex = lastIndex; newIndex < data.size(); newIndex++){
                if((Timestamp.valueOf(data.get(newIndex).get("time").toString())).getTime() > a1L) break;
            }

            double error = 0.0;
            double area = 0.0;

//            System.out.println("last index:" + lastIndex);
//            System.out.println("new index:" + newIndex);

            for(int j = lastIndex; j < newIndex; j++){
                Long x0L = (Timestamp.valueOf(data.get(j-1).get("time").toString())).getTime();
                double x0 = x0L.doubleValue();
                Long x1L = (Timestamp.valueOf(data.get(j).get("time").toString())).getTime();
                double x1 = x1L.doubleValue();
                double y0, y1;

                value = data.get(j-1).get(label);
                if(value instanceof Double) y0 = (Double) value;
                else if(value instanceof Integer) y0 = ((Integer) value).doubleValue();
                else if(value instanceof Long) y0 = ((Long) value).doubleValue();
                else y0 = (Double) value;

                value = data.get(j).get(label);
                if(value instanceof Double) y1 = (Double) value;
                else if(value instanceof Integer) y1 = ((Integer) value).doubleValue();
                else if(value instanceof Long) y1 = ((Long) value).doubleValue();
                else y1 = (Double) value;

                area += (x1 - x0) * (Math.abs(y0) + Math.abs(y1)) / 2;

                // 线性插值
                double dy0 = (x0 - a0) * (b1 - b0) / (a1 - a0) + b0 - y0;
                double dy1 = (x1 - a0) * (b1 - b0) / (a1 - a0) + b0 - y1;

//                System.out.println("a0: " + a0);
//                System.out.println("b0: " + b0);
//                System.out.println("a1: " + a1);
//                System.out.println("b1: " + b1);
//                System.out.println("x0: " + x0);
//                System.out.println("x1: " + x1);
//                System.out.println("y0: " + y0);
//                System.out.println("y1: " + y1);
//                System.out.println("dy0: " + dy0);
//                System.out.println("dy1: " + dy1);

                boolean isCross = (dy0 >= 0) != (dy1 >= 0);

                if(isCross){
                    double x = x0 + (x1 - x0) * (Math.abs(dy0) / (Math.abs(dy0) + Math.abs(dy1)));
//                    System.out.println(Math.abs(dy0) * (x - x0) / 2 + Math.abs(dy1) * (x1 - x) / 2);
                    error += Math.abs(dy0) * (x - x0) / 2 + Math.abs(dy1) * (x1 - x) / 2;
                }
                else {
//                    System.out.println(Math.abs((x1 - x0) * (Math.abs(dy0) + Math.abs(dy1)) / 2));
                    error += Math.abs((x1 - x0) * (Math.abs(dy0) + Math.abs(dy1)) / 2);
                }
            }

            lastIndex = newIndex;

//            System.out.println("error" + error);
//            System.out.println("area" + area);

            if(Double.isNaN(error)) error = 0.0;
            if(Double.isNaN(area)) area = 0.0;

            sample.get(i).put("error", error);
            sample.get(i).put("area", area);
        }

        System.out.println("the time stamp comparasion:");
        System.out.println("raw data time:" + data.get(0).get("time"));
        System.out.println("raw data value:" + data.get(0).get(label));
        System.out.println("sample data time:" + sample.get(0).get("time"));
        System.out.println("sample data timestamp:" + sample.get(0).get("timestamp"));
        System.out.println("sample data value:" + sample.get(0).get(label));
        System.out.println("<<<<<<<<<<<<<<<<<<");

    }
}
