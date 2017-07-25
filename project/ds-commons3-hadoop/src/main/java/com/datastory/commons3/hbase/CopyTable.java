package com.datastory.commons3.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.List;


/**
 * com.datastory.commons3.hbase.tesst
 *
 * @author zhaozhen
 * @since 2017/5/31
 */

public class CopyTable extends Configured implements Tool{

     static class HBaseCopyMap extends TableMapper<ImmutableBytesWritable, Put>{

          @Override
          protected void map(ImmutableBytesWritable key, Result value,
                             Context context) throws IOException, InterruptedException {
               List<KeyValue> kvs =  value.list();
               Put p = new Put(key.get());
               for(KeyValue kv : kvs)
                    p.add(kv);
               context.write(key, p);
          }

     }

     public static Job createSubmittableJob(Configuration conf, String[] args)throws IOException{

               String srcHbaseTable = args[0];
               String dstHbaseTable = args[1];
               Scan scan = new Scan();
               scan.setCaching(500);
               scan.setCacheBlocks(false);
               Job job = new Job(conf, "COPY_TABLE");
               job.setJarByClass(CopyTable.class);
               job.setNumReduceTasks(0);
               TableMapReduceUtil.initTableMapperJob(srcHbaseTable, scan, HBaseCopyMap.class, ImmutableBytesWritable.class, Result.class, job);
               TableMapReduceUtil.initTableReducerJob(dstHbaseTable, null, job);
               return job;
     }

     public int run(String[] args) throws Exception{
               Job job = createSubmittableJob(getConf(), args);
               return job.waitForCompletion(true)? 0 : 1;
     }

     public static void main(String[] args) throws Exception{
          int res = ToolRunner.run(new CopyTable(), args);
          System.exit(res);
     }

}
