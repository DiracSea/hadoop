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
    public void write2HDFS(String src, String dst) throws IOException {
        Configuration conf = new Configuration();
        FSDataOutputStream out = null;

        FileSystem fileSystem = FileSystem.get(conf);

        // Check if the file already exists
        Path path = new Path(src);
        InputStream in = null;
        if (fileSystem.exists(path)) {
            System.out.println("File already exists");
            return;
        }
        // Hadoop
        // Create a new file and write data to it.
        try {
            out = fileSystem.create(new Path(dst));
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

        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = null;

        try {
            // open the path
            in = fs.open(new Path(dst));
            IOUtils.copyBytes(in, System.out, 4096, false);
            System.out.println("END");
        }
        catch (IOException e){
            System.out.printf("IOException!!\n");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        finally {
            IOUtils.closeStream(in);
        }
        fs.close();
    }

    public void randomAccess(String dst) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = null;
        fs.open(new Path(dst));

        try {
            in = fs.open(new Path(dst));
        }
        catch (Exception e) {
            System.out.printf("File not exist!!\n");
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
        for (int i = 0; i < 2000; i++) {
            Random rand = null;
            int pos = rand.nextInt(2070000000) + 1;
            in.seek(pos);
            IOUtils.copyBytes(in, System.out, 1024, false);

            System.out.println("END " + i);
        }

        in.close();
        fs.close();
    }
}
