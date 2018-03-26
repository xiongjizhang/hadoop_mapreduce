package com.hadoop.leftjoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhao on 2018-02-26.
 */
public class Pair implements Writable, WritableComparable<Pair> {
    private Text key;
    private Text value;

    public Pair() {
        this.key = new Text();
        this.value = new Text();
    }

    public Pair(Text key, Text value){
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(Pair o) {
        return this.key.compareTo(o.key);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.key.write(dataOutput);
        this.value.write(dataOutput);
        // dataOutput.writeUTF(this.key.toString());
        // dataOutput.writeUTF(this.value.toString());
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.key.readFields(dataInput);
        this.value.readFields(dataInput);
    }

    public Text getKey(){
        return key;
    }

    public void setKey(Text key){
        this.key = key;
    }

    public Text getValue(){
        return value;
    }

    public void setValue(Text value) {
        this.value = value;
    }
}
