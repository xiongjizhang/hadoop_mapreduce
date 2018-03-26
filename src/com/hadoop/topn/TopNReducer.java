package com.hadoop.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.DoubleSummaryStatistics;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by zhao on 2018-02-10.
 */
public class TopNReducer  extends Reducer<NullWritable, Text, NullWritable, Text> {

    private int n = 10; // д╛хо

    protected void setup(Reducer.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        n = conf.getInt("top.n", 10);
    }

    public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        SortedMap<Double, String> finalTopN = new TreeMap<Double, String>();
        for (Text cat : values) {
            String[] tokens = cat.toString().split(",");
            Double weight = Double.parseDouble(tokens[0]);
            finalTopN.put(weight, cat.toString());

            if (finalTopN.size() > n){
                finalTopN.remove(finalTopN.firstKey());
            }
        }

        for (String cat : finalTopN.values()){
            context.write(NullWritable.get(), new Text(cat));
        }
    }

}
