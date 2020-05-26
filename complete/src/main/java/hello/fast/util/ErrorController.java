package hello.fast.util;

import java.sql.Timestamp;
import java.util.*;

/**
 * 层级采样面积误差计算类，利用分段线性插值，计算采样结果与原始数据的面积误差
 */
public class ErrorController {

    private static Comparator<Map<String, Object>> sampleComparator = new Comparator<Map<String, Object>>(){
        @Override
        public int compare(Map<String, Object> sampleDataPoint1, Map<String, Object> sampleDataPoint2){
            long t1 = (Timestamp.valueOf(sampleDataPoint1.get("time").toString())).getTime();
            long t2 = (Timestamp.valueOf(sampleDataPoint2.get("time").toString())).getTime();
            return Math.round(t1-t2);
        }
    };

    public static void lineError(List<Map<String, Object>> data, List<Map<String, Object>> sample, String label, boolean isRawData){

        if(sample.size() < 1) return;
        sample.get(0).put("error", 0.0);
        sample.get(0).put("area", 0.0);
        if(sample.size() == 1) return;

        int lastIndex = 1;
        int newIndex;
        for(int i = 1; i < sample.size(); i++){
            // 将时间与数值间隔转化为double类型以便计算
            double a0, a1;
            Long a0L = (Timestamp.valueOf(sample.get(i-1).get("time").toString())).getTime();
            a0 = a0L.doubleValue();
            Long a1L = (Timestamp.valueOf(sample.get(i).get("time").toString())).getTime();
            a1 = a1L.doubleValue();

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

            // 找到每两个样本点之间对应的原始数据
            for(newIndex = lastIndex; newIndex < data.size(); newIndex++){
                if((Timestamp.valueOf(data.get(newIndex).get("time").toString())).getTime() > a1L) break;
            }

            double error = 0.0;
            double area = 0.0;
            double weight = 0.0;

            // 按原始数据点分段计算误差面积与原始数据面积
            for(int j = lastIndex; j < newIndex; j++){
                // 将时间与数值间隔转化为double类型以便计算
                double x0, x1;
                Long x0L = (Timestamp.valueOf(data.get(j-1).get("time").toString())).getTime();
                x0 = x0L.doubleValue();
                Long x1L = (Timestamp.valueOf(data.get(j).get("time").toString())).getTime();
                x1 = x1L.doubleValue();

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

                if(isRawData) {
                    area += (x1 - x0) * (Math.abs(y0) + Math.abs(y1)) / 2;
                }
                else {
                    area += (Double)data.get(j).get("area");
                    // 除L0层级外，更高层级样本误差需累加已有层级样本的误差
                    error += (Double)data.get(j).get("error");
                }

                // 新逻辑：高层级样本点的权重为低层级样本权重和
                weight += (Double)data.get(j).get("weight");

                // 线性插值
                double dy0 = (x0 - a0) * (b1 - b0) / (a1 - a0) + b0 - y0;
                double dy1 = (x1 - a0) * (b1 - b0) / (a1 - a0) + b0 - y1;

                // 判断样本折线图与原始数据折线段是否相交
                boolean isCross = (dy0 >= 0) != (dy1 >= 0);
                if(isCross){
                    // 相交，误差面积为两三角形面积
                    double x = x0 + (x1 - x0) * (Math.abs(dy0) / (Math.abs(dy0) + Math.abs(dy1)));
                    error += Math.abs(dy0) * (x - x0) / 2 + Math.abs(dy1) * (x1 - x) / 2;
                }
                else {
                    // 不相交，误差面积为矩形面积
                    error += Math.abs((x1 - x0) * (Math.abs(dy0) + Math.abs(dy1)) / 2);
                }
            }

            lastIndex = newIndex;

            // 重合数据点异常处理
            if(Double.isNaN(error)) error = 0.0;
            if(Double.isNaN(area)) area = 0.0;

            sample.get(i).put("error", error);
            sample.get(i).put("area", area);
            sample.get(i).put("weight", weight);
        }

    }
}
