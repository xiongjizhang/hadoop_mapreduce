package com.hadoop.leftjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by zhao on 2018-02-26.
 */
public class LeftJoinDriver extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "LeftJoinDriver");
        // job.setJobName("SecondarySortDriver");
        job.setJarByClass(LeftJoinDriver.class);

        Path inputTransactionPath = new Path(strings[0]+"\\transaction.txt");
        Path inputUserPath = new Path(strings[0]+"\\user.txt");
        Path outputPath = new Path(strings[1]);
        // FileInputFormat.setInputPaths(job, inputPath);
        MultipleInputs.addInputPath(job,inputTransactionPath, TextInputFormat.class, LeftJoinTransactionMapper.class);
        MultipleInputs.addInputPath(job, inputUserPath, TextInputFormat.class, LeftJoinUserMapper.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Pair.class);
        job.setMapOutputValueClass(Pair.class);

        // job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(LeftJoinReducer.class);
        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setGroupingComparatorClass(SecondarySortGroupComparator.class);

        boolean flag = job.waitForCompletion(true);
        System.out.println("Run Status : " + flag);
        return flag ? 1 : 0;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2){
            throw new IllegalArgumentException("Usage SecondarySort <inputPath>,<outputPath>");
        }
        int runStatus = ToolRunner.run(new LeftJoinDriver(), args);
        System.exit(runStatus);
    }
}
