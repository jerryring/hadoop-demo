package io.hadoop.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * created by cj on 2019/4/15
 */
public class GenerateSequenceFile {
    public static void main(String[] args) throws IOException {
        //1.sequenceFile文件是通过SequenceFile类生成的
        //createWriter方法参数:conf:hadop配置项,name:文件名,
        //keyClass:key的数据类型;valClass:值得数据类型
        //指定文件名称
        Writer.Option name = Writer.file(new Path("file:/e:/sf"));
        //指定key的类型
        Writer.Option keyClass = Writer.keyClass(LongWritable.class);
        //指定value的类型
        Writer.Option valClass = Writer.valueClass(Text.class);
        //hadoop配置项
        Configuration conf = new Configuration();
        //创建输出流
        Writer writer = SequenceFile.createWriter(conf, name, keyClass, valClass);

        //读取文本文件
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(new Path("file:/E:/Personal/hadoop-root/word.txt"));
        String line = null;
        Long num = 0L;
        while ((line = in.readLine())!=null) {
            //不断递增key值
            num++;
            //输出每行数据到sequencefile中
            writer.append(new LongWritable(num), new Text(line));
        }

        IOUtils.closeStream(writer);
    }
}
