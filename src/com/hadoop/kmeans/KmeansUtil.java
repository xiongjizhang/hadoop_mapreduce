package com.hadoop.kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhao on 2018-03-08.
 */
public class KmeansUtil {

    public static List<List<Double>> getCentersByPath(String path, Configuration conf) {
        List<List<Double>> result = new ArrayList<List<Double>>();

        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path in = new Path(path+"\\part-r-00000");
            FSDataInputStream fsIn = hdfs.open(in);
            LineReader lineIn = new LineReader(fsIn, conf);
            Text line = new Text();
            while (lineIn.readLine(line) > 0){
                String record = line.toString();
                if (record == null || record.trim().length() == 0 || record.trim().substring(0,3).equals("crc")) {
                    continue;
                }
                /*
                因为Hadoop输出键值对时会在键跟值之间添加制表符，
                所以用空格代替之。
                */
                String[] fields = record.replace("\t", " ").split(" ");
                List<Double> tmplist = new ArrayList<Double>();
                for (int i = 0; i < fields.length; ++i){
                    tmplist.add(Double.parseDouble(fields[i]));
                }
                result.add(tmplist);
            }
            fsIn.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void deletePath(String pathName, Configuration conf) {
        try {
            FileSystem hdfs = FileSystem.get(conf);
            Path path = new Path(pathName);
            hdfs.delete(path, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static boolean isFinished(String oldPath, String newPath, int k, double threshold, Configuration conf) {
        List<List<Double>> oldCenters = KmeansUtil.getCentersByPath(oldPath, conf);
        List<List<Double>> newCenters = KmeansUtil.getCentersByPath(newPath, conf);

        double distance = 0;
        for (int i = 0; i < k; i++) {
            for (int j = 1; j < newCenters.get(0).size(); j++) {
                distance += Math.pow(Math.abs(newCenters.get(i).get(j)-oldCenters.get(i).get(j)), 2);
            }
        }

        if (distance < threshold)
            return true;

        /*
        如果不满足终止条件，则用本次迭代的聚类中心更新聚类中心
        */
        try {
            KmeansUtil.deletePath(oldPath, conf);
            FileSystem hdfs = FileSystem.get(conf);
            hdfs.copyToLocalFile(new Path(newPath), new Path(oldPath));
            // hdfs.copyToLocalFile(new Path(newpath), new Path("/home/hadoop/class/oldcenter.data"));
            // hdfs.delete(new Path(oldpath), true);
            // hdfs.moveFromLocalFile(new Path("/home/hadoop/class/oldcenter.data"), new Path(oldpath));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

}
