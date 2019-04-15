package io.hadoop.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * created by cj on 2019/4/15
 */
public class Temperature {
    /**
     * mapper逻辑
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineArr = line.split("\t");
            String year = lineArr[0].substring(0, 4);
            context.write(new Text(year), new Text(lineArr[0] + ":" + lineArr[1]));
        }
    }

    /**
     * Reducer处理逻辑
     * 求取每年气温最大值
     */
    public static class MyReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
            double min = Long.MIN_VALUE;
            String date = null;
            for (Text value : values) {
                String tempStr = value.toString();
                String[] tempArr = tempStr.split(":");
                double temp = Double.parseDouble(tempArr[1]);
                if (min < temp) {
                    min = temp;
                    date = tempArr[0];
                }
            }
            context.write(new Text(date), new DoubleWritable(min));
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
        Job job = Job.getInstance(conf, "temperature");
        //通过类名打成jar包
        job.setJarByClass(Temperature.class);
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
        job.setOutputValueClass(DoubleWritable.class);

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
