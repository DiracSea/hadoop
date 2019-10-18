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
        long t1, t2, t3, t4, t5, t6;
        try {
            long start=System.currentTimeMillis();
            f.write2HDFS(src, dst);
            long end=System.currentTimeMillis();
            t1 = end - start;
            long start1=System.currentTimeMillis();
            f.readfromHDFS(dst);
            long end1=System.currentTimeMillis();
            t2 = end - start;
            long start2=System.currentTimeMillis();
            f.randomAccess(dst);
            long end2=System.currentTimeMillis();
            t3 = end - start;
            f.flag = 1;
            long start3=System.currentTimeMillis();
            f.write2HDFS(src, dst1);
            long end3=System.currentTimeMillis();
            t4 = end - start;
            long start4=System.currentTimeMillis();
            f.readfromHDFS(dst1);
            long end4=System.currentTimeMillis();
            t5 = end - start;
            long start5=System.currentTimeMillis();
            f.randomAccess(dst1);
            long end5=System.currentTimeMillis();
            t6 = end - start;
            System.out.println("HDFS");
            System.out.println("Total copy time "+(t1)+"ms");
            System.out.println("Total read time "+(t2)+"ms");
            System.out.println("Total random access time "+(t3)+"ms");
            System.out.println("Local");
            System.out.println("Total copy time "+(t4)+"ms");
            System.out.println("Total read time "+(t5)+"ms");
            System.out.println("Total random time "+(t6)+"ms");

        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

}
