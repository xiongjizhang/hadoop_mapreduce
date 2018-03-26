package com.hadoop.common_friends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhao on 2018-03-05.
 */
public class CommonFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        // <user>,<friend1> <friend2> ... <friendk>
        if (value == null || value.toString().trim().length() == 0)
            return;

        String[] tokens = value.toString().trim().split(",");
        String[] friends = tokens[1].trim().split(" ");
        String user = tokens[0];
        for (String friend : friends) {
            if (user.compareTo(friend) < 0) {
                context.write(new Text(user + ", " + friend), new Text(tokens[1]));
            } else {
                context.write(new Text(friend + ", " + user), new Text(tokens[1]));
            }
        }
    }

}
