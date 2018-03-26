package com.hadoop.friend_recommendation;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhao on 2018-03-06.
 */
public class FriendRecommendationReducer extends Reducer<Text, Text, Text, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, List<String>> mutualFriends = new HashMap<String, List<String>>();

        for (Text tuple : values) {
            String[] tokens = tuple.toString().trim().split(",");
            String friend = tokens[0];
            String mutualFriend = tokens[1];
            boolean isFriend = "-1".equals(mutualFriend);

            if (isFriend) {
                mutualFriends.put(friend, null);
            } else {
                if (mutualFriends.containsKey(friend)) {
                    /*if (mutualFriends.get(friend) == null) {
                        List<String> n = new ArrayList<>();
                        n.add(mutualFriend);
                        mutualFriends.put(friend, n);
                    } else */
                    if (mutualFriends.get(friend) != null && !mutualFriends.get(friend).contains(mutualFriend))
                        mutualFriends.get(friend).add(mutualFriend);
                } else {
                    List<String> n = new ArrayList<>();
                    n.add(mutualFriend);
                    mutualFriends.put(friend, n);
                }
            }
        }

        context.write(key, new Text(buildOutput(mutualFriends)));

    }

    String buildOutput(Map<String, List<String>> mutualFriends) {
        StringBuffer buffer = new StringBuffer();
        for (Map.Entry<String, List<String>> mutualFriend : mutualFriends.entrySet()) {
            String key = mutualFriend.getKey();
            List<String> values = mutualFriend.getValue();
            if (values == null)
                continue;
            buffer.append(key);
            buffer.append(" (" + values.size() + ": " + values.toString() + "), ");
        }
        return buffer.toString();
    }

}
