package com.hadoop.moving_average;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhao on 2018-02-28.
 */
public class CompositeKey implements Writable, WritableComparable<CompositeKey> {

    private String name;
    private long timestamp;

    public CompositeKey() {
        name = "";
        timestamp = 0;
    }

    public CompositeKey(String name, long timestamp) {
        this.name = name;
        this.timestamp = timestamp;
    }

    public static CompositeKey copy(CompositeKey tsd) {
        return new CompositeKey(tsd.name, tsd.timestamp);
    }

    @Override
    public int compareTo(CompositeKey o) {
        if (this.name.compareTo(o.name) != 0) {
            return this.name.compareTo(o.name);
        } else if ( this.timestamp != o.timestamp) {
            return this.timestamp < o.timestamp ? -1 : 1;
        } else {
            return 0;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.name);
        dataOutput.writeLong(this.timestamp);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.name = dataInput.readUTF();
        this.timestamp = dataInput.readLong();
    }

    /*public static class CompositeKeyComparator extends WritableComparator {
        public CompositeKeyComparator() {
            super(CompositeKey.class);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return compareBytes(b1, s1, l1, b2, s2, l2);
        }
    }

    static { // register this comparator
        WritableComparator.define(CompositeKey.class,
                new CompositeKeyComparator());
    }*/

    public String toString() {
        return "(" + name + ", " + DateUtil.getDateAsString(timestamp) + ")";
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
