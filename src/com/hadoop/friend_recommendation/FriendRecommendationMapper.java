package com.hadoop.friend_recommendation;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import java.io.IOException;

/**
 * Created by zhao on 2018-03-06.
 */
public class FriendRecommendationMapper extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        // <user>, <friend1> <friend2> ... <friendk>
        String line = value.toString();
        if (line == null || line.length() == 0) {
            return ;
        }

        String[] tokens = line.trim().split(",");
        String user = tokens[0];
        String[] friends = tokens[1].trim().split(" ");

        for (String friend : friends) {
            context.write(new Text(user), new Text(friend + ",-1"));
        }

        for (int i = 0; i < friends.length; i++) {
            for (int j = i+1; j < friends.length; j++) {
                // Text[] tupleStr = {new Text(friends[i]), new Text(user)};
                // TupleWritable tuple = new TupleWritable(tupleStr);
                context.write(new Text(friends[j]), new Text(friends[i] + "," + user));

                // Text[] tupleStr2 = {new Text(friends[j]), new Text(user)};
                // TupleWritable tuple2 = new TupleWritable(tupleStr2);
                context.write(new Text(friends[i]), new Text(friends[j] + "," + user));
            }
        }

    }

}
