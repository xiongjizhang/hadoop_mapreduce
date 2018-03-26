package com.hadoop.moving_average;

import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by zhao on 2018-02-28.
 */
public class NaturalKeyPartitioner extends Partitioner<CompositeKey, TimeSeriesData> {
    @Override
    public int getPartition(CompositeKey compositeKey, TimeSeriesData timeSeriesData, int i) {
        return Math.abs((int)hash(compositeKey.getName()) % i);
    }

    static long hash(String str) {
        long h = 1125899906842597L;
        int length = str.length();
        for (int i = 0; i < length; i++) {
            h = 31*h+str.charAt(i);
        }
        return h;
    }
}
