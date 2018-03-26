package com.hadoop.relative_frequency;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by zhao on 2018-02-27.
 */
public class RelativeFrequencyCombiner extends Reducer<PairOfWords, IntWritable, PairOfWords, IntWritable> {
    @Override
    protected void reduce(PairOfWords key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        //
        int partialSum = 0;
        for (IntWritable value : values) {
            partialSum += value.get();
        }
        //
        context.write(key, new IntWritable(partialSum));
    }
}
