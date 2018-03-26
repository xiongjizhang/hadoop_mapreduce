package com.hadoop.demo1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhao on 2017-12-15.
 */
public class DemoMapper extends Mapper<LongWritable,Text,Text,Text> {

    public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] linesplit = line.split(" ");
        String _id = linesplit[0];
        String _price = linesplit[1];
        String _date = linesplit[2];

        context.write(new Text(_id),new Text(_date + "_" + _price));
    }
}
