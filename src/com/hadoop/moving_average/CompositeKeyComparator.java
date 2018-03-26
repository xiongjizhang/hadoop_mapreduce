package com.hadoop.moving_average;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by zhao on 2018-02-28.
 */
public class CompositeKeyComparator extends WritableComparator{

    public CompositeKeyComparator(){
        super(CompositeKey.class, true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKey key1 = (CompositeKey) a;
        CompositeKey key2 = (CompositeKey) b;

        int comparison = key1.getName().compareTo(key2.getName());
        if (comparison == 0) {
            if (key1.getTimestamp() == key2.getTimestamp()) {
                comparison = 0;
            } else if (key1.getTimestamp() < key2.getTimestamp()) {
                comparison = -1;
            } else {
                comparison = 1;
            }
        }
        return comparison;
    }

}
