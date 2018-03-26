package com.hadoop.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by zhao on 2018-01-18.
 */
public class DateTemperaturePartitioner extends Partitioner<DateTemperaturePair, Text> {
    @Override
    public int getPartition(DateTemperaturePair dateTemperaturePair, Text text, int i) {
        return Math.abs(dateTemperaturePair.getYearMonth().hashCode() % i);
    }
}
