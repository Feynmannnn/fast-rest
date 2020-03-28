package com.company;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
	// write your code here
        List<Integer> weights = new ArrayList<>();
        weights.add(0);
        double alpha = 0.99999;
        for(int i = 1; i < 10; i++){
            int w = i-1;
            weights.add(w);
            weights.set(i-1, Math.max(w, weights.get(i-1)));
        }
//        System.out.println(weights);
        System.out.println((int)(alpha * 480000));
    }
}
