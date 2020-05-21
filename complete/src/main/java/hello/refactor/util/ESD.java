package hello.refactor.util;

import java.util.List;

public class ESD {
    public static int ESDOutlier(List<Double> data, int outlierLimit){
        int n = data.size();
        for(int i = 0; i < outlierLimit; i++){
            double mean = getMean(data, n - i);
            double std = getStd(data, n - i);
            double candi = data.get(n-i-1);
            double R = (candi - mean) / std;
            double t = 1.96;
            double C = (n - i) * t / (Math.sqrt(n - i - 1 + t * t) * (n - i + 1));
            if(R > C) return i + 1;
        }
        return outlierLimit;
    }

    static double getMean(List<Double> data, int range){
        double sum = 0;
        for(int i = 0; i < range; i++){
            sum += data.get(i);
        }
        return sum / range;
    }

    static double getStd(List<Double> data, int range){
        double sum = 0;
        double mean = getMean(data, range);
        for(int i = 0; i < range; i++){
            sum += (data.get(i) - mean) * (data.get(i) - mean);
        }
        return Math.sqrt(sum / range);
    }
}
