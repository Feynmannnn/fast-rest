package hello.refactor.util;

import java.util.ArrayList;
import java.util.List;

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
        System.out.println("mean" + mean);
        double stdDev = getStdDev(values);
        System.out.println("stdDev" + stdDev);

        int res = 0;

        final List<Double> newList = new ArrayList<>();

        for (double value : values) {
            boolean isLessThanLowerBound = value < mean - stdDev * scaleOfElimination;
            boolean isGreaterThanUpperBound = value > mean + stdDev * scaleOfElimination;
            boolean isOutOfBounds = isLessThanLowerBound || isGreaterThanUpperBound;

            if (isOutOfBounds) res++;
        }

        return res;
    }


}
