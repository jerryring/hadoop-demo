package io.hadoop.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * created by cj on 2019/4/12
 */
public class HdfsCore {
    public static void main(String[] args) {
//        FileSystem fs = getHadoopFileSystem();
//        System.out.println(fs);


//        createPath("/test2");
        createFile("test.txt");
    }

    public static FileSystem getHadoopFileSystem() {
        FileSystem fs = null;
        Configuration configuration = new Configuration();
        //core-site.xml
        //执行NameNode访问地址
        configuration.set("fs.defaultFS", "hdfs://hadoop.senior01.test.com:9000");
        try {
            fs = FileSystem.get(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fs;
    }

    public static boolean createPath(String path) {
        boolean result = false;
        FileSystem fs = getHadoopFileSystem();
        try {
            result = fs.mkdirs(new Path(path));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    public static boolean createFile(String pathName) {
        boolean res = false;
        FileSystem fs = getHadoopFileSystem();
        try {
            FSDataOutputStream out = fs.create(new Path("/test", pathName));

            res = true;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return res;
    }
}
