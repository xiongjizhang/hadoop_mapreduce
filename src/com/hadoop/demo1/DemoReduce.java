package com.hadoop.demo1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhao on 2017-12-15.
 */
public class DemoReduce extends Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterable<Text> values, Context context){
        List<String> list = new ArrayList<>();
        for (Text value : values){
            list.add(value.toString());
        }
    }
}
