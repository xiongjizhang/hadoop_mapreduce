package com.hadoop.movie_recommendation;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhao on 2018-03-06.
 */
public class MovieRecommendationsReducer1 extends Reducer<Text, Text, NullWritable, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int ratingCount = 0;
        List<String> list = new ArrayList<>();
        for (Text value : values) {
            ratingCount++;
            list.add(value.toString());
        }
        for (String value : list) {
            String[] tokens = value.trim().split(",");
            context.write(NullWritable.get(), new Text(tokens[0] + "," + key.toString() + "," + tokens[1] + "," + ratingCount));
        }
    }

}
