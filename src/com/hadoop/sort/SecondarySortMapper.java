package com.hadoop.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhao on 2018-01-18.
 */
public class SecondarySortMapper extends Mapper<LongWritable, Text, DateTemperaturePair, IntWritable> {

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(",");
        String yearMonth = values[0]+values[1];
        String day = values[2];
        int temperature = Integer.parseInt(values[3]);
        DateTemperaturePair dateTemperaturePair = new DateTemperaturePair();
        dateTemperaturePair.setYearMonth(new Text(yearMonth));
        dateTemperaturePair.setDay(new Text(day));
        dateTemperaturePair.setTemperature(new IntWritable(temperature));

        context.write(dateTemperaturePair, new IntWritable(temperature));
    }

}
