package com.hadoop.leftjoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by zhao on 2018-02-26.
 */
public class SecondarySortGroupComparator extends WritableComparator {

    @Override
    public int compare(WritableComparable wc1, WritableComparable wc2){
        Pair pair1 = (Pair) wc1;
        Pair pair2 = (Pair) wc2;

        return pair1.getKey().compareTo(pair2.getKey());
    }
}
