package com.hadoop.moving_average;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.Time;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhao on 2018-02-28.
 */
public class TimeSeriesData implements Writable, WritableComparable<TimeSeriesData> {

    private long timestamp;
    private double value;

    public TimeSeriesData(){
        timestamp = 0;
        value = 0;
    }

    public static TimeSeriesData copy(TimeSeriesData tsd) {
        return new TimeSeriesData(tsd.timestamp, tsd.value);
    }

    public TimeSeriesData(long timestamp, double value) {
        this.timestamp = timestamp;
        this.value = value;
    }

    @Override
    public int compareTo(TimeSeriesData o) {
        if (this.timestamp == o.getTimestamp()) {
            return 0;
        } else if (this.timestamp < o.getTimestamp()) {
            return -1;
        } else {
            return 1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(this.timestamp);
        dataOutput.writeDouble(this.value);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.timestamp = dataInput.readLong();
        this.value = dataInput.readDouble();
    }

    public String toString(){
        return "(" + DateUtil.getDateAsString(timestamp) + ", " + value + ")";
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }
}
