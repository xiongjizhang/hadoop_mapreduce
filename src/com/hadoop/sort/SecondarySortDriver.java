package com.hadoop.sort;

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

/**
 * Created by zhao on 2018-01-19.
 */
public class SecondarySortDriver extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "SecondarySortDriver");
        // job.setJobName("SecondarySortDriver");
        job.setJarByClass(SecondarySortDriver.class);

        Path inputPath = new Path(strings[0]);
        Path outputPath = new Path(strings[1]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setOutputKeyClass(DateTemperaturePair.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(DateTemperaturePair.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setPartitionerClass(DateTemperaturePartitioner.class);
        job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);

        boolean flag = job.waitForCompletion(true);
        System.out.println("Run Status : " + flag);
        return flag ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2){
            throw new IllegalArgumentException("Usage SecondarySort <inputPath>,<outputPath>");
        }
        int runStatus = ToolRunner.run(new SecondarySortDriver(),args);
        System.exit(runStatus);
    }
}
