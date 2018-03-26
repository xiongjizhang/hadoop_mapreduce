package com.hadoop.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhao on 2018-03-08.
 */
public class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    private int k;
    private List<List<Double>> centers;

    public void setup(Mapper.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        k = conf.getInt("center.num", 3);
        String centerPath = conf.get("center.path","");
        centers = KmeansUtil.getCentersByPath(centerPath, conf);
    }

    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        if (value.toString() == null || value.toString().trim().length() == 0) {
            return;
        }
        String[] tokens = value.toString().trim().split(",");
        double minDistance = Double.MAX_VALUE;
        int index = 0;
        for (int i = 0; i < k; i++) {
            double distance = 0;
            for (int j = 0; j < tokens.length; j++) {
                distance += Math.pow(Math.abs(Double.parseDouble(tokens[j])-centers.get(i).get(j+1)), 2);
            }
            if (distance < minDistance) {
                index = i;
                minDistance = distance;
            }
        }
        context.write(new IntWritable(index), value);
    }

}
