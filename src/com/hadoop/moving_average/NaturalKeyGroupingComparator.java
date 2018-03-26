package com.hadoop.moving_average;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by zhao on 2018-02-28.
 */
public class NaturalKeyGroupingComparator extends WritableComparator {

    protected NaturalKeyGroupingComparator(){
        super(CompositeKey.class,true);
    }

    public int compare(WritableComparable a, WritableComparable b) {
        CompositeKey key1 = (CompositeKey) a;
        CompositeKey key2 = (CompositeKey) b;

        return key1.getName().compareTo(key2.getName());

    }
}
