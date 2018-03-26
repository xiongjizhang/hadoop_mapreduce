package com.hadoop.common_friends;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by zhao on 2018-03-05.
 */
public class CommonFriendsReducer extends Reducer<Text, Text, Text, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        String[] tokens1 = iter.next().toString().trim().split(" ");
        String[] tokens2 = iter.next().toString().trim().split(" ");
        StringBuffer commonFriends = new StringBuffer();
        for (String p1 : tokens1) {
            int flag = 0;
            for (String p2 : tokens2) {
                if (p1.equals(p2)) {
                    flag = 1;
                    break;
                }
            }
            if (flag == 1) {
                commonFriends.append(p1 + " ");
            }
        }

        context.write(key, new Text(commonFriends.toString().trim()));

    }

}
