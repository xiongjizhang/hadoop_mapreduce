package com.hadoop.movie_recommendation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhao on 2018-03-06.
 */
public class MovieRecommendationsMapper1 extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        // <user>,<movie>,<rating>
        if (value.toString() == null || value.toString().trim().length() == 0) {
            return;
        }

        String[] tokens = value.toString().trim().split(",");
        context.write(new Text(tokens[1]), new Text(tokens[0] + "," + tokens[2]));
    }

}
