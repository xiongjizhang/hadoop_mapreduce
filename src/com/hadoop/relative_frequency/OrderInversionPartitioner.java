package com.hadoop.relative_frequency;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by zhao on 2018-02-27.
 */
public class OrderInversionPartitioner extends Partitioner<PairOfWords, IntWritable> {
    @Override
    public int getPartition(PairOfWords pairOfWords, IntWritable intWritable, int i) {
        return Math.abs(pairOfWords.getLeftWord().hashCode() % i);
    }
}
