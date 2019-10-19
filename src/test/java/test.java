/*
 * @Author: Longze Su
 * @Date: 2019-10-18 12:39:22
 * @Description: CS211_Project1
 * @LastEditTime: 2019-10-18 21:02:36
 * @LastEditors: Longze Su
 */
import java.io.*;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public class test {
    public static void main(String[] args) {
        file2HDFS f = new file2HDFS();
        String dst = "hdfs://localhost:9000/input/water.csv";
        String src = "/data/water.csv";
        String dst2 = "/data/water1.csv";
        String output = "/data/cs226/output.txt";
        long start=System.currentTimeMillis();
        f.randomAccess(dst);
        long end=System.currentTimeMillis();
        long t6 = end - start;   
    }
}