package com.hadoop.movie_recommendation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhao on 2018-03-07.
 */
public class MovieRecommendationsMapper2 extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        // <user>,<movie>,<rating>,<count>
        if (value.toString() == null || value.toString().trim().length() == 0 || value.toString().trim().substring(0,3).equals("crc")) {
            return;
        }

        String[] tokens = value.toString().trim().split(",");
        context.write(new Text(tokens[0]), new Text(tokens[1] + "," + tokens[2] + "," + tokens[3]));
    }

}
