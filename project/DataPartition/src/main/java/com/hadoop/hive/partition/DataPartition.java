package com.hadoop.hive.partition;

/**
 * Created by GOD on 2016/8/2.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by GOD on 2016/7/18.
 */
public class DataPartition {
    public static class PartitonMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text ssj = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

        }
    }

    public static class PatitionReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "DataPartition");
        job.setJarByClass(DataPartition.class); //注意，必须添加这行，否则hadoop无法找到对应的class
        //控制map()和reduce()函数输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //指定map和reduce的类型
        job.setMapperClass(PartitonMap.class);
        job.setReducerClass(PatitionReduce.class);
        //控制输入类型
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        //控制输出和是输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));  //输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  //输出路径

        job.waitForCompletion(true);//提交作业并等待执行完成
    }
}