package io.hadoop.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/**
 * created by cj on 2019/4/16
 */
public class MapJoin {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

        //存放商品小表数据<item_id,item_type>
        private Map<String, String> itemInfoMap = new HashMap<>();

        /**
         * 将小表中所有记录加载到mapper进程机器内存中
         */
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            //1.读取磁盘空间上的对应小表
            //废弃
            //URI[] paths = DistributedCache.getCacheFiles(context.getConfiguration());
            URI[] paths = context.getCacheFiles();
            for (URI uri : paths) {
                String pathName = uri.toString();
                //判断是否是iteminfo小表
                if (!pathName.endsWith("iteminfo")) return;
                //通过输入流读取磁盘上的文件
                BufferedReader reader = new BufferedReader(new FileReader(pathName));
                String str = null;
                //遍历文件中的每行
                while ((str = reader.readLine()) != null) {
                    String[] itemInfoArr = str.split(" ");
                    if (itemInfoArr.length == 2) {
                        //存放<item_id,item_type>
                        itemInfoMap.put(itemInfoArr[0], itemInfoArr[1]);
                    }
                }
            }
        }

        ;

        /**
         * map处理逻辑
         * 通过读取大表中的每条记录
         */
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            //判断是否是大表数据,需要获取到输入分片的文件名，和大表的文件名进行比对
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            if (fileName.endsWith("detail")) {
                //将分片中的每一条记录与小表中所有记录进行合并
                String detail = value.toString();
                String[] detailArr = detail.split(" ");
                if (detailArr.length != 3) return;
                String itemType = itemInfoMap.get(detailArr[1]);
                if (itemType == null) return;
                //拼接输出的value,输出格式：<item_id,item_type+ "\t" + order_id + "\t" + amount>
                StringBuffer sb = new StringBuffer();
                sb.append(itemType).append("\t").append(detailArr[0]).append("\t").append(detailArr[2]);
                //输出数据
                context.write(new Text(detailArr[1]), new Text(sb.toString()));
            }
        }
    }

    //驱动方法
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //0.创建一个Job
        Configuration conf = new Configuration();

        //判断输出目录是否存在，如果存在就删除
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[2]))) {
            fs.delete(new Path(args[2]), true);
        }

        //将小表加载到各个Mapper进程所在机器的磁盘上(废弃)
//		DistributedCache.addCacheFile(new Path(args[1]).toUri(), conf);

        Job job = Job.getInstance(conf, "map-join");
        //通过类名打成jar包
        job.setJarByClass(MapJoin.class);

        //1.输入文件
        FileInputFormat.addInputPath(job, new Path(args[0]));

        //Job#addCacheFile(URI)}
        //将小表加载到各个Mapper进程所在机器的磁盘上
        job.addCacheFile(new Path(args[1]).toUri());
        //2.编写mapper处理逻辑
        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //3.设置reduce个数
        job.setNumReduceTasks(0);

        //4.输出文件
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        //6.运行Job
        boolean result = job.waitForCompletion(true);
        System.out.println(result ? 1 : 0);
    }
}
