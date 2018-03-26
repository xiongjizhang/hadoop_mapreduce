package com.hadoop.relative_frequency;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhao on 2018-02-27.
 */
public class RelativeFrequencyMapper extends Mapper<LongWritable, Text, PairOfWords, IntWritable> {

    private int neighborWindow = 2;
    private PairOfWords pair = new PairOfWords();

    public void setup(Mapper.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        neighborWindow = conf.getInt("neighbor.window", 2);
    }

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] tokens = value.toString().split(" ");
        if (tokens.length < 2)
            return;

        for (int i = 0; i < tokens.length; i++) {
            String word = tokens[i];
            pair.setLeftWord(new Text(word));
            int start = i - neighborWindow < 0 ? 0 : i-neighborWindow;
            int end = i + neighborWindow >= tokens.length ? tokens.length - 1 : i + neighborWindow;
            for (int j = start; j <= end; j++) {
                if (i==j)
                    continue;
                pair.setRightWord(new Text(tokens[j]));
                context.write(pair, new IntWritable(1));
            }
            pair.setRightWord(new Text("*"));
            context.write(pair, new IntWritable(end-start));
        }
        // context.write(key, value);
    }

}
