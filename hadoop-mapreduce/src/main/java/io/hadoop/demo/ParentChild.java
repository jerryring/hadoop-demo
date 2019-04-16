package io.hadoop.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 单表关联
 * created by cj on 2019/4/16
 */
public class ParentChild {
    /**
     * mapper逻辑
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String temp = value.toString();
            String[] tempArray = temp.split(" ");
            if (!"child".equals(tempArray[0])) {
                //输出格式<child,parent:1>
                context.write(new Text(tempArray[0]), new Text(tempArray[1] + ":" + 1));
                //输出格式<parent,child:2>
                context.write(new Text(tempArray[1]), new Text(tempArray[0] + ":" + 2));
            }
        }
    }

    /**
     * Reducer处理逻辑
     */
    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            List<String> childList = new ArrayList<>();
            List<String> parentList = new ArrayList<>();
            //1找出孙子辈列表
            //2找出祖父辈列表
            for (Text value : values) {
                String tempVar = value.toString();
                String[] tempVarArr = tempVar.split(":");
                if (tempVarArr[1].equals("1")) {
                    parentList.add(tempVarArr[0]);
                } else if (tempVarArr[1].equals("2")) {
                    childList.add(tempVarArr[0]);
                }
            }
            for (String child : childList) {
                for (String parent : parentList) {
                    context.write(new Text(child), new Text(parent));
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //0.创建job
        Configuration conf = new Configuration();
        //远程调用运行配置hdfs
        //conf.set("fs.defaultFS","hdfs://hadoop.senior01.test.com:9000");

        // 配置输入输出参数
        // 输出到本地文件系统：file:/E:/Personal/hadoop-root/word.txt file:/E:/Personal/hadoop-root/output
        // 输出到hdfs：file:/E:/Personal/hadoop-root/word.txt output         （output:/user/chenjia/out）
        Job job = Job.getInstance(conf, "parent-child");
        //通过类名打成jar包
        job.setJarByClass(ParentChild.class);
        //1.输入文件

//        for (int i = 0; i < args.length - 1; i++) {
        FileInputFormat.addInputPath(job, new Path(args[0]));
//        }

        //默认为TextInputFormat
        //指定SequenceFileInputFormat
//        job.setInputFormatClass(SequenceFileInputFormat.class);


        //2.编写mapper流程
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);


        //3.编写shuffle


        //4.编写reduce流程
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //5.输出文件

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
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
