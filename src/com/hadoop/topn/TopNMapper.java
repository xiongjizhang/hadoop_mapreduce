package com.hadoop.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.DoubleSummaryStatistics;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by zhao on 2018-02-10.
 */
public class TopNMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private SortedMap<Double, String> topNMap = new TreeMap<Double, String>();
    private int n = 10; // д╛хо

    public void setup(Mapper.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        n = conf.getInt("top.n", 10);
    }

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(",");
        Double weight = Double.parseDouble(tokens[0]);
        topNMap.put(weight, value.toString());

        if (topNMap.size() > n){
            topNMap.remove(topNMap.firstKey());
        }
        // context.write(key, value);
    }

    public void cleanup(Mapper.Context context) throws IOException, InterruptedException {
        for (String cat : topNMap.values()){
            context.write(NullWritable.get(), new Text(cat));
        }
    }
}
