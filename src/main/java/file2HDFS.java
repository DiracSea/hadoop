/*
 * @Author: Longze Su
 * @Date: 2019-10-18 11:51:30
 * @Description: CS211_Project1
 * @LastEditTime: 2019-10-18 22:58:47
 * @LastEditors: Longze Su
 */
// Longze Su
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

public class file2HDFS {
    static int flag = 0;
    public void write2HDFS(String src, String dst) throws IOException {
        Configuration conf = new Configuration();
        FSDataOutputStream out = null;
        FileSystem fileSystem = null;
        InputStream in = null;

        if (flag == 0)
            fileSystem = FileSystem.get(conf);
        else
            fileSystem = FileSystem.getLocal(conf).getRawFileSystem();
        // Check if the file already exists
        Path path = new Path(dst);
        if (fileSystem.exists(path)) {
            System.out.println("File already exists");
            return;
        }
        // Hadoop
        // Create a new file and write data to it.
        try {
            out = fileSystem.create(path);
        }
        catch (Exception e) {
            System.out.printf("Dest file not exist!!\n");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        try {
            in = new BufferedInputStream(new FileInputStream(
                    new File(src)));
        }
        catch (FileNotFoundException e) {
            System.out.printf("Source file not exist!!\n");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        IOUtils.copyBytes(in, out, 4096, true);
        System.out.println(dst + " copied to HDFS");

        in.close();
        out.close();
        fileSystem.close();
    }

    public void readfromHDFS(String dst) throws IOException {
        Configuration conf = new Configuration();

        Path path = new Path(dst);
        FileSystem fs = path.getFileSystem(conf);

        FSDataInputStream in = null;
        // FSDataOutputStream out = null;

        try {
            // open the path4
            in = fs.open(new Path(dst));
            byte buffer[] = new byte[1024];

            while (in.read(buffer) > 0) {
                ;
            }
            System.out.println("END");
        }
        catch (IOException e){
            System.out.printf("IOException!!\n");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(fs);
        }
    }

    public void randomAccess(String dst) throws IOException {
        Configuration conf = new Configuration();
        
        Path path = new Path(dst);
        FileSystem fs = path.getFileSystem(conf);

        FSDataInputStream in = null;
        
        Random rand = new Random(); long pos = 0;

        try {
            in = fs.open(path);
            byte buffer[] = new byte[1024];

            for (int i = 0; i < 2000; i++) {

                pos = rand.nextInt(2070000000) + 1;
                in.readFully(pos, buffer, 0, 1024);
            }


            while (in.read(buffer) > 0) {
                ;
            }
            System.out.println("END");
        }
        catch (IOException e){
            System.out.printf("IOException!!\n");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(fs);
        }
    }
}
