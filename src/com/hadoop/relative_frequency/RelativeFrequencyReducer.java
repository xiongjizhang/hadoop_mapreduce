package com.hadoop.relative_frequency;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by zhao on 2018-02-27.
 */
public class RelativeFrequencyReducer extends Reducer<PairOfWords, IntWritable, PairOfWords, DoubleWritable> {

    private int totalCount = 0;
    private String currentWord = "NOT_DEFINED";

    protected void reduce(PairOfWords key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        if (key.getRightWord().toString().equals("*")) {
            if (currentWord.equals(key.getLeftWord().toString())) {
                totalCount += getTotalCount(values);
            } else {
                currentWord = key.getLeftWord().toString();
                totalCount = getTotalCount(values);
            }
        } else {
            int count = getTotalCount(values);
            double pl = ((double) count) / totalCount;
            context.write(key, new DoubleWritable(pl));
        }

    }

    private int getTotalCount(Iterable<IntWritable> values){
        int count = 0;
        for (IntWritable value : values){
            count += value.get();
        }
        return count;
    }
}
