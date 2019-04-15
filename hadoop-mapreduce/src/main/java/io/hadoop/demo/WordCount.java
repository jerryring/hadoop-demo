package io.hadoop.demo;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * created by cj on 2019/4/12
 */
public class WordCount {
    static {
//        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.0-cdh5.9.0");
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        //0.创建job
        Configuration conf = new Configuration();
        //远程调用运行配置hdfs
        //conf.set("fs.defaultFS","hdfs://hadoop.senior01.test.com:9000");

        // 配置输入输出参数
        // 输出到本地文件系统：file:/E:/Personal/hadoop-root/word.txt file:/E:/Personal/hadoop-root/output
        // 输出到hdfs：file:/E:/Personal/hadoop-root/word.txt output         （output:/user/chenjia/out）
        Job job = Job.getInstance(conf, "word-count");
        //通过类名打成jar包
        job.setJarByClass(WordCount.class);
        //1.输入文件
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //默认为TextInputFormat
        //指定SequenceFileInputFormat
        job.setInputFormatClass(SequenceFileInputFormat.class);


        //2.编写mapper流程
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);


        //3.编写shuffle


        //4.编写reduce流程
        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //5.输出文件

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //序列化文件的压缩类型:None, Block, Record
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        //序列化文件的压缩格式:default, gzip, lz4, snappy
        SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        //6.运行
        boolean res = job.waitForCompletion(true);
        System.out.println(res);
    }

    /**
     * 默认MapReduce是通过过TextInputFormat进行切片 交给Mapper进行处理
     * TextInputFormat:key:当前行的首字母索引，value:当前行数据
     * Mapper类参数：输入key类型:Long,输入value类型：String,输出 key类型：String，输出value类型：Long
     * MapReduce为了网络传输时序列化文件比较小，执行速度快，对基本类型进行包装，实现自己的序列化
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        LongWritable one = new LongWritable(1);

        /**
         * 每行数据拆分
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String words = value.toString();
            String[] wordsArr = words.split(" ");
            for (String s : wordsArr) {
                context.write(new Text(s), one);
            }
        }
    }

    /**
     * 进行全局聚合
     * Reducer参数：输入key类型：String,输入Value类型：Long，输出key类型：String，输出value类型：Long
     */
    public static class MyReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
        /**
         * 将map输出结果进行聚合
         * key:单词，values:个数[1，1，1]
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            Long sum = 0L;
            for (LongWritable value : values) {
                //累加单词
                sum += value.get();
            }
            //输出最终结果
            context.write(key, new LongWritable(sum));
        }
    }

}
