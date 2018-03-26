package com.hadoop.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by zhao on 2018-01-19.
 */
public class SecondarySortReducer extends Reducer<DateTemperaturePair, IntWritable, Text, Text> {

    @Override
    public void reduce(DateTemperaturePair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        String valueStr = "";
        for (IntWritable value : values) {
            valueStr = valueStr + value.toString() + ",";
        }
        System.out.println(key.getYearMonth() + " " + valueStr);
        context.write(key.getYearMonth(), new Text(valueStr));
    }

}
