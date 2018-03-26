package com.hadoop.market_basket_analyze;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.log4j.Logger;

/**
 * Created by zhao on 2018-03-02.
 */
public class MBADriver extends Configured implements Tool {

    private static final Logger THE_LOGGER = Logger.getLogger(MBADriver.class);

    @Override
    public int run(String[] args) throws Exception {
        int numberOfPairs = 2;
        if (args.length > 2) {
            numberOfPairs = Integer.parseInt(args[2]);
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = getConf();
        conf.setInt("number.of.pairs", numberOfPairs);
        Job job = Job.getInstance(conf, "MBA");

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // set mapper/reducer
        job.setMapperClass(MBAMapper.class);
        job.setReducerClass(MBAReducer.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        THE_LOGGER.info("Job Finished in milliseconds: " + (System.currentTimeMillis() - startTime));
        return 0;
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 2){
            throw new IllegalArgumentException("Usage: MBADriver <inputFile>,<outputFile>,[windowSize]");
        }
        int runStatus = ToolRunner.run(new MBADriver(), args);
        System.exit(runStatus);
    }
}
