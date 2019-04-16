package io.hadoop.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * created by cj on 2019/4/16
 */
public class ReduceJoin {
    /**
     * mapper逻辑
     */
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {
        /**
         * map处理逻辑
         * 1.判断是哪个表
         * 2.针对不同的表输出不同的数据
         */
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            //1.判断是哪个表文件
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            //2.切分每行数据
            String line = value.toString();
            if (line.contains("item_id")) return;
            String[] lineArr = line.split(" ");
            //输出格式的1：订单明细表；2：商品表
            if ("detail".equals(fileName)) {
                //订单明细表,输出格式<item_id,"1:order_id:amount">
                context.write(new Text(lineArr[1]), new Text("1\t" + lineArr[0] + "\t" + lineArr[2]));
            } else if ("iteminfo".equals(fileName)) {
                //商品表,输出格式<item_id,"2:item_type">
                context.write(new Text(lineArr[0]), new Text("2\t" + lineArr[1]));
            }
        }
    }

    /**
     * Reducer处理逻辑
     */
    public static class MyReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * reduce处理逻辑
         * 1.将相同商品id的订单明细信息和商品信息进行拆分,拆分后存到响应的订单明细列表和商品列表中
         * 2.将订单明细列表和商品列表进行嵌套遍历
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                              Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            //0.定义订单明细列表和商品信息列表
            List<String> orderDetailList = new ArrayList<>();
            List<String> itemInfoList = new ArrayList<>();
            //1.将相同商品id的订单明细信息和商品信息进行拆分,拆分后存到响应的订单明细列表和商品列表中
            for (Text tempVal : values) {
                String tempValStr = tempVal.toString();
                String[] tempValArr = tempValStr.split("\t");
                if ("1".equals(tempValArr[0])) {
                    //订单明细表
                    orderDetailList.add(tempValStr.substring(2));
                } else {
                    //商品表
                    itemInfoList.add(tempValArr[1]);
                }
            }
            //2.将订单明细列表和商品列表进行嵌套遍历
            for (String itemInfo : itemInfoList) {
                for (String orderDetail : orderDetailList) {
                    context.write(key, new Text(itemInfo + "\t" + orderDetail));
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
        Job job = Job.getInstance(conf, "reduce-join");
        //通过类名打成jar包
        job.setJarByClass(ReduceJoin.class);
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
        job.setReducerClass(MyReducer.class);
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
