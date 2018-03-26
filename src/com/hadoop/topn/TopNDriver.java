package com.hadoop.topn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by zhao on 2018-01-19.
 */
public class TopNDriver extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();

        // conf.setInt(MRJobConfig.NUM_MAPS,1);
        if (strings.length > 2){
            conf.setInt("top.n",Integer.parseInt(strings[2]));
        }

        Job job = Job.getInstance(conf, "SecondarySortDriver");
        // job.setJobName("SecondarySortDriver");
        job.setJarByClass(TopNDriver.class);

        Path inputPath = new Path(strings[0]);
        Path outputPath = new Path(strings[1]);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        // job.setMapOutputKeyClass(DateTemperaturePair.class);
        // job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);
        // job.setPartitionerClass(DateTemperaturePartitioner.class);
        // job.setGroupingComparatorClass(DateTemperatureGroupingComparator.class);

        boolean flag = job.waitForCompletion(true);
        System.out.println("Run Status : " + flag);
        return flag ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2){
            throw new IllegalArgumentException("Usage SecondarySort <inputPath>,<outputPath>");
        }
        int runStatus = ToolRunner.run(new TopNDriver(),args);
        System.exit(runStatus);
    }
}
