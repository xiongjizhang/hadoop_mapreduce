package com.hadoop.leftjoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhao on 2018-02-26.
 */
public class LeftJoinTransactionMapper extends Mapper<LongWritable, Text, Pair, Pair> {

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        String userId = values[2];
        String productId = values[1];

        context.write(new Pair(new Text(userId), new Text("2")), new Pair(new Text("P"), new Text(productId)));
    }

}
