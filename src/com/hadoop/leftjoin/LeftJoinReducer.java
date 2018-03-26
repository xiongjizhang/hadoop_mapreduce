package com.hadoop.leftjoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by zhao on 2018-02-26.
 */
public class LeftJoinReducer extends Reducer<Pair, Pair, Text, Text> {

    @Override
    public void reduce(Pair key, Iterable<Pair> values, Context context) throws IOException, InterruptedException {
        String locationId = "undefined";
        for (Pair value : values) {
            System.out.println(value.getKey().toString() + " " + value.getValue().toString());
            if (value.getKey().toString().equals("L")) {
                locationId = value.getValue().toString();
                continue;
            }
            String productId = value.getValue().toString();
            context.write(new Text(productId), new Text(locationId));
        }
    }
}
