package io.hadoop.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * created by cj on 2019/4/15
 */
public class DiscinctAndSort {

    //Mapper处理逻辑
    //读取每行数据，转换为<Text,"">
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, new Text(""));
        }
    }

    //Reducer处理逻辑
    //进行数据去重
    public static class MyReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }

    //驱动方法
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //0.创建job
        Configuration conf = new Configuration();
        //远程调用运行配置hdfs
        //conf.set("fs.defaultFS","hdfs://hadoop.senior01.test.com:9000");

        // 配置输入输出参数
        // 输出到本地文件系统：file:/E:/Personal/hadoop-root/word.txt file:/E:/Personal/hadoop-root/output
        // 输出到hdfs：file:/E:/Personal/hadoop-root/word.txt output         （output:/user/chenjia/out）
        Job job = Job.getInstance(conf, "distinct");
        //通过类名打成jar包
        job.setJarByClass(DiscinctAndSort.class);
        //1.输入文件

        for (int i = 0; i < args.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(args[i]));
        }

        //默认为TextInputFormat
        //指定SequenceFileInputFormat
//        job.setInputFormatClass(SequenceFileInputFormat.class);


        //2.编写mapper流程
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        //3.编写shuffle


        //4.编写reduce流程
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //5.输出文件

        FileOutputFormat.setOutputPath(job, new Path(args[args.length - 1]));
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //序列化文件的压缩类型:None, Block, Record
        //SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        //序列化文件的压缩格式:default, gzip, lz4, snappy
        //SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        //6.运行
        boolean res = job.waitForCompletion(true);
        System.out.println(res);

    }
}
