package com.hadoop.kmeans;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhao on 2018-03-08.
 */
public class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<List<Double>> data = new ArrayList<List<Double>>();
        for (Text value : values) {
            List<Double> vector = new ArrayList<Double>();
            String[] tokens = value.toString().trim().split(",");
            for (int i = 0; i < tokens.length; i++) {
                vector.add(Double.parseDouble(tokens[i]));
            }
            data.add(vector);
        }
        String result = "";
        for (int j = 0; j < data.get(0).size(); j++) {
            double sum = 0;
            for (int i = 0; i < data.size(); i++) {
                sum += data.get(i).get(j);
            }
            if (j == 0) {
                result = "" + sum/data.size();
            } else {
                result = result + " " + (sum/data.size());
            }
        }
        context.write(key, new Text(result));
    }

}
