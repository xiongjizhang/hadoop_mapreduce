package com.hadoop.relative_frequency;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by zhao on 2018-02-27.
 */
public class PairOfWords implements Writable, WritableComparable<PairOfWords> {

    private Text leftWord;
    private Text rightWord;

    public PairOfWords() {
        leftWord = new Text();
        rightWord = new Text();
    }

    public PairOfWords(Text left, Text right) {
        this.leftWord = left;
        this.rightWord = right;
    }

    @Override
    public int compareTo(PairOfWords o) {
        int comp = this.leftWord.compareTo(o.leftWord);
        if (comp == 0) {
            comp = this.rightWord.compareTo(o.rightWord);
        }
        return comp;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.leftWord.write(dataOutput);
        this.rightWord.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.leftWord.readFields(dataInput);
        this.rightWord.readFields(dataInput);
    }

    @Override
    public String toString() {
        return "(" + leftWord + ", " + rightWord + ")";
    }

    public Text getLeftWord() {
        return leftWord;
    }

    public void setLeftWord(Text leftWord) {
        this.leftWord = leftWord;
    }

    public Text getRightWord() {
        return rightWord;
    }

    public void setRightWord(Text rightWord) {
        this.rightWord = rightWord;
    }
}
