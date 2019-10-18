// Longze.Su

import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public class run {
    public static void main(String[] args) {
        String dst = "hdfs://localhost:9000/input/water.csv";
        String src = "/data/water.csv";
        String dst1 = "/data/water1.csv";
        file2HDFS f = new file2HDFS();
        System.out.println("Local");
        long start=System.currentTimeMillis();
        try {
            f.write2HDFS(src, dst);
        long end=System.currentTimeMillis();
        System.out.println("Total copy time "+(end-start)+"ms");
        long start1=System.currentTimeMillis();   //获取开始时间
        f.readfromHDFS(dst);
        long end1=System.currentTimeMillis();
        System.out.println("Total copy time "+(end1-start1)+"ms");
        long start2=System.currentTimeMillis();
        f.randomAccess(dst);
        long end2=System.currentTimeMillis();
        System.out.println("Total copy time "+(end2-start2)+"ms");

        System.out.println("HDFS");
        long start3=System.currentTimeMillis();
        f.write2HDFS(src, dst1);
        long end3=System.currentTimeMillis();
        System.out.println("Total copy time "+(end3-start3)+"ms");
        long start4=System.currentTimeMillis();   //获取开始时间
        f.readfromHDFS(dst1);
        long end4=System.currentTimeMillis();
        System.out.println("Total copy time "+(end4-start4)+"ms");
        long start5=System.currentTimeMillis();
        f.randomAccess(dst1);
        long end5=System.currentTimeMillis();
        System.out.println("Total copy time "+(end5-start5)+"ms");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
