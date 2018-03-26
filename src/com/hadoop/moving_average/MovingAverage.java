package com.hadoop.moving_average;

/**
 * Created by zhao on 2018-02-28.
 */
public class MovingAverage {

    private int windowSize;
    private double[] values;
    private long index;

    public MovingAverage(int windowSize) {
        this.windowSize = windowSize;
        values = new double[windowSize];
        index = 0;
    }

    public void addValue(double value) {
        values[(int)index % windowSize] = value;
        index++;
    }

    public double getAverageValue() {
        if (index < windowSize) {
            return sum(this.values)/index;
        } else {
            return sum(this.values)/windowSize;
        }
    }

    private double sum(double[] values) {
        double s = 0;
        for (double value : values) {
            s += value;
        }
        return s;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(int windowSize) {
        this.windowSize = windowSize;
    }

    public double[] getValues() {
        return values;
    }

    public void setValues(double[] values) {
        this.values = values;
    }
}
