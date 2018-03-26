package com.hadoop.moving_average;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by zhao on 2018-02-28.
 */
public class SortByMRF_MovingAverageReducer extends Reducer<CompositeKey, TimeSeriesData, Text, Text> {

    private int windowSize = 3;

    protected void reduce(CompositeKey key, Iterable<TimeSeriesData> values, Context context) throws IOException, InterruptedException {

        MovingAverage ma = new MovingAverage(windowSize);

        for (TimeSeriesData data : values) {
            ma.addValue(data.getValue());
            context.write(new Text(key.getName()), new Text(DateUtil.getDateAsString(data.getTimestamp()) + "," + ma.getAverageValue()));
        }

    }

    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        windowSize = conf.getInt("window.size", 3);
    }

}
