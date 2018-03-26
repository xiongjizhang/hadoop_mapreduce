package com.hadoop.movie_recommendation;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhao on 2018-03-07.
 */
public class MovieRecommendationsReducer2 extends Reducer<Text, Text, NullWritable, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> list = new ArrayList<>();
        for (Text value : values) {
            list.add(value.toString());
        }

        for (int i = 0; i < list.size() ; i++) {
            for (int j = i+1; j < list.size(); j++) {
                String[] v1 = list.get(i).trim().split(",");
                String[] v2 = list.get(j).trim().split(",");
                if (v1[0].compareTo(v2[0]) < 0) {
                    String result = v1[0] + "," + v2[0] + ":" + v1[1] + " " + v1[2] + " " + v2[1] + " " + v2[2];
                    context.write(NullWritable.get(), new Text(result));
                } else {
                    String result = v2[0] + "," + v1[0] + ":" + v2[1] + " " + v2[2] + " " + v1[1] + " " + v1[2];
                    context.write(NullWritable.get(), new Text(result));
                }
            }
        }
    }

}
