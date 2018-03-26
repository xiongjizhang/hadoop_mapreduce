package com.hadoop.moving_average;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * Created by zhao on 2018-02-28.
 */
public class SortByMRF_MovingAverageDriver extends Configured implements Tool {

    private static final Logger THE_LOGGER = Logger.getLogger(SortByMRF_MovingAverageDriver.class);

    @Override
    public int run(String[] args) throws Exception {
        int windowSize = 3;
        if (args.length > 2) {
            windowSize = Integer.parseInt(args[0]);
        }

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);

        Configuration conf = getConf();
        conf.setInt("window.size", windowSize);
        Job job = Job.getInstance(conf, "SortByMRF_MovingAverage");

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // set mapper/reducer
        job.setMapperClass(SortByMRF_MovingAverageMapper.class);
        job.setReducerClass(SortByMRF_MovingAverageReducer.class);

        // define mapper's output key-value
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(TimeSeriesData.class);

        // define reducer's output key-value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(NaturalKeyPartitioner.class);
        job.setGroupingComparatorClass(NaturalKeyGroupingComparator.class);

        long startTime = System.currentTimeMillis();
        job.waitForCompletion(true);
        THE_LOGGER.info("Job Finished in milliseconds: " + (System.currentTimeMillis() - startTime));
        return 0;
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 2){
            throw new IllegalArgumentException("Usage: RelativeFrequency <inputFile>,<outputFile>,[windowSize]");
        }
        int runStatus = ToolRunner.run(new SortByMRF_MovingAverageDriver(), args);
        System.exit(runStatus);
    }
}
