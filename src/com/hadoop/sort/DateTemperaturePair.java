package com.hadoop.sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhao on 2018-01-18.
 */
public class DateTemperaturePair implements Writable, WritableComparable<DateTemperaturePair> {

    private Text yearMonth;
    private Text day;
    private IntWritable temperature;

    public DateTemperaturePair(){
        this.yearMonth = new Text();
        this.day = new Text();
        this.temperature = new IntWritable();
    }

    public DateTemperaturePair(Text yearMonth, Text day, IntWritable temperature) {
        this.yearMonth = yearMonth;
        this.day = day;
        this.temperature = temperature;
    }

    @Override
    public int compareTo(DateTemperaturePair o) {
        int compareValue = this.yearMonth.compareTo(o.getYearMonth());
        if ( compareValue == 0 ){
            compareValue = this.temperature.compareTo(o.getTemperature());
        }
        return compareValue;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.yearMonth.toString());
        dataOutput.writeUTF(this.day.toString());
        dataOutput.writeInt(this.temperature.get());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        yearMonth = new Text(dataInput.readUTF());
        day = new Text(dataInput.readUTF());
        temperature = new IntWritable(dataInput.readInt());
    }

    public Text getDay() {
        return day;
    }

    public void setDay(Text day) {
        this.day = day;
    }

    public IntWritable getTemperature() {
        return temperature;
    }

    public void setTemperature(IntWritable temperature) {
        this.temperature = temperature;
    }

    public Text getYearMonth() {
        return yearMonth;
    }

    public void setYearMonth(Text yearMonth) {
        this.yearMonth = yearMonth;
    }
}
