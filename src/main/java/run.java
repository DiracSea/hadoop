/*
 * @Author: Longze Su
 * @Date: 2019-10-18 09:49:25
 * @Description: CS211_Project1
 * @LastEditTime: 2019-10-18 22:44:00
 * @LastEditors: Longze Su
 */
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
        String dst1 = "file:///data/water1.csv";
        // String dst3 = "/data/water2.csv";
        /// String dst1 = "hdfs://localhost:9000/input/water1.csv";
        String output = "/data/cs226/output.txt"; 
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
            t2 = end1 - start1;

            long start2=System.currentTimeMillis();
            f.randomAccess(dst);
            long end2=System.currentTimeMillis();
            t3 = end2 - start2;

            f.flag = 1;
            long start3=System.currentTimeMillis();
            f.write2HDFS(src, dst1);
            long end3=System.currentTimeMillis();
            t4 = end3 - start3;
            
            long start4=System.currentTimeMillis();
            f.readfromHDFS(src);
            long end4=System.currentTimeMillis();
            t5 = end4 - start4;

            long start5=System.currentTimeMillis();
            f.randomAccess(src);
            long end5=System.currentTimeMillis();
            t6 = end5 - start5;

            System.out.println("HDFS");
            System.out.println("Total copy time "+(t1)+"ms");
            System.out.println("Total read time "+(t2)+"ms");
            System.out.println("Total random access time "+(t3)+"ms");
            System.out.println("Local");
            System.out.println("Total copy time "+(t4)+"ms");
            System.out.println("Total read time "+(t5)+"ms");
            System.out.println("Total random time "+(t6)+"ms");
			File writename = new File(output); 
			writename.createNewFile(); 
			BufferedWriter out = new BufferedWriter(new FileWriter(writename));
            out.write("HDFS\r\n"); 
            out.write("Total copy time "+(t1)+"ms\r\n"); 
            out.write("Total read time "+(t2)+"ms\r\n"); 
            out.write("Total random access time "+(t3)+"ms\r\n"); 
            out.write("Local\r\n"); 
            out.write("Total copy time "+(t4)+"ms\r\n"); 
            out.write("Total read time "+(t5)+"ms\r\n"); 
            out.write("Total random time "+(t6)+"ms\r\n"); 
            out.flush(); 
            out.close();

        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

}
