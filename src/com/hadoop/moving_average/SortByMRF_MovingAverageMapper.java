package com.hadoop.moving_average;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhao on 2018-02-28.
 */
public class SortByMRF_MovingAverageMapper extends Mapper<LongWritable, Text, CompositeKey, TimeSeriesData> {

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        // <name>,<date>,<value>
        String record = value.toString();
        if ((record == null) || (record.length() == 0)) {
            return;
        }
        String[] tokens = record.split(",");

        try {
            CompositeKey compositeKey = new CompositeKey(tokens[0], DateUtil.getDateAsMilliSeconds(tokens[1]));
            TimeSeriesData data  = new TimeSeriesData(DateUtil.getDateAsMilliSeconds(tokens[1]), Double.parseDouble(tokens[2]));
            context.write(compositeKey, data);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
