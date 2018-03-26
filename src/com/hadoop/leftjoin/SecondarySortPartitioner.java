package com.hadoop.leftjoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by zhao on 2018-02-26.
 */
public class SecondarySortPartitioner extends Partitioner<Pair, Pair> {
    @Override
    public int getPartition(Pair key, Pair value, int i) {
        return Math.abs(key.getKey().hashCode() % i);
    }
}
