package hello.refactor.util;

import java.util.List;

/**
 * 离群点检测类，利用Z-score自动检测数据序列中的离群值个数
 */
public class OutlierDetection {
    public static double getMean(List<Double> values) {
        if(values == null || values.size() < 1) return 0;

        double sum = 0;
        for (double value : values) {
            sum += value;
        }

        return (sum / values.size());
    }

    public static double getVariance(List<Double> values) {
        if(values == null || values.size() < 1) return 0;

        double mean = getMean(values);
        double temp = 0;

        for (double a : values) {
            temp += (a - mean) * (a - mean);
        }

        return temp / (values.size() - 1);
    }

    public static double getStdDev(List<Double> values) {
        if(values == null || values.size() < 1) return 0;

        return Math.sqrt(getVariance(values));
    }

    public static int zscoreOutlierNum(List<Double> values, float scaleOfElimination){
        double mean = getMean(values);
        double stdDev = getStdDev(values);

        int res = 0;

        for (double value : values) {
            boolean isLessThanLowerBound = value < mean - stdDev * scaleOfElimination;
            boolean isGreaterThanUpperBound = value > mean + stdDev * scaleOfElimination;
            boolean isOutOfBounds = isLessThanLowerBound || isGreaterThanUpperBound;

            if (isOutOfBounds) res++;
        }

        return res;
    }
}
