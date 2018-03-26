package com.hadoop.movie_recommendation;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by zhao on 2018-03-07.
 */
public class MovieRecommendationsReducer3 extends Reducer<Text, Text, Text, Text> {

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // rating1 count1 rating2 count2

        double dotProduct = 0;
        double rating1Sum = 0;
        double rating2Sum = 0;
        double rating1Norm = 0;
        double rating2Norm = 0;
        int size = 0;
        int maxCount1 = 0;
        int maxCount2 = 0;
        for (Text value : values) {
            String[] tokens = value.toString().trim().split(" ");
            double rating1 = Double.parseDouble(tokens[0]);
            double rating2 = Double.parseDouble(tokens[2]);
            dotProduct += rating1*rating2;
            rating1Sum += rating1;
            rating2Sum += rating2;
            rating1Norm += rating1*rating1;
            rating2Norm += rating2*rating2;
            size++;
            if (maxCount1 < Integer.parseInt(tokens[1])) {
                maxCount1 = Integer.parseInt(tokens[1]);
            }
            if (maxCount2 < Integer.parseInt(tokens[3])) {
                maxCount2 = Integer.parseInt(tokens[3]);
            }
        }

        double pearsonCor = calPearson(size,dotProduct, rating1Sum, rating2Sum, rating1Norm, rating2Norm);

        double consineCor = calConsine(dotProduct, rating1Norm, rating2Norm);

        double jaccardCor = calJaccard(size, maxCount1, maxCount2);

        context.write(key, new Text(pearsonCor + " " + consineCor + " " + jaccardCor));
    }

    private double calPearson(int size, double dotProduct, double rating1Sum, double rating2Sum, double rating1Norm, double rating2Norm) {
        return (size*dotProduct - rating1Sum*rating2Sum)/Math.sqrt(size*rating1Norm-rating1Sum*rating1Sum)/Math.sqrt(size*rating2Norm-rating2Sum*rating2Sum);
    }

    private double calConsine(double dotProduct, double rating1Norm, double rating2Norm) {
        return dotProduct/Math.sqrt(rating1Norm)/Math.sqrt(rating2Norm);
    }

    private double calJaccard(int size, int maxCount1, int maxCount2) {
        return ((double) size)/(maxCount1 + maxCount2 - size);
    }

}
