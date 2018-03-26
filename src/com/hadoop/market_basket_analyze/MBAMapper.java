package com.hadoop.market_basket_analyze;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhao on 2018-03-02.
 */
public class MBAMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private int numberOfPairs;

    private static final Text reducerKey = new Text();

    private static final IntWritable NUMBER_ONE = new IntWritable(1);

    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        numberOfPairs = conf.getInt("number.of.pairs", 2);
    }

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        List<String> items = convertItemsToList(line);
        if (items == null || items.isEmpty()) {
            return;
        }

        generateMapperOutput(numberOfPairs, items, context);
    }

    private void generateMapperOutput(int numberOfPairs, List<String> items, Context context) throws IOException, InterruptedException {
        List<List<String>> sortedCombinations = Combination.findSortedCombinations(items, numberOfPairs);
        for (List<String> itemList : sortedCombinations) {
            reducerKey.set(itemList.toString());
            context.write(reducerKey, NUMBER_ONE);
        }
    }

    private static List<String> convertItemsToList(String line) {
        List<String> items = new ArrayList<String>();
        if (line == null || line.length() == 0) {
            return null;
        }
        String[] tokens = line.split(",");
        if (tokens == null || tokens.length == 0) {
            return null;
        }
        for (String item : tokens) {
            if (item != null){
                items.add(item);
            }
        }
        return items;
    }

}
