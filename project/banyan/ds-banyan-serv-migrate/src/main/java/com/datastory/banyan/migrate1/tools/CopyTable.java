package com.datastory.banyan.migrate1.tools;

import com.datastory.banyan.base.RhinoETLConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.List;


/**
 * com.datastory.banyan.migrate1.tools.CopyTable
 *
 * @author zhaozhen
 * @since 2017/5/31
 */

public class CopyTable {

    public static class HBaseCopyMap extends TableMapper<ImmutableBytesWritable, Put> {

        @Override
        protected void map(ImmutableBytesWritable key, Result value,
                           Context context) throws IOException, InterruptedException {
            List<KeyValue> kvs = value.list();
            Put p = new Put(key.get());
            for (KeyValue kv : kvs)
                p.add(kv);
            context.write(key, p);
        }

    }

    public static Job createSubmittableJob(Configuration conf, String srcHbaseTable, String dstHbaseTable) throws IOException {
        String appName = CopyTable.class.getName() + " : " + srcHbaseTable + " -> " + dstHbaseTable;

        conf.set("hbase.client.keyvalue.maxsize", "" + (50 * 1024 * 1024));
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapred.reduce.slowstart.completed.maps", "1.0");  // map跑完100% 才跑reducer
        conf.set("mapreduce.job.running.map.limit", "200");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.map.memory.mb", "1024");

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        Job job = new Job(conf, appName);
        job.setJarByClass(CopyTable.class);
        job.setNumReduceTasks(0);
        TableMapReduceUtil.initTableMapperJob(srcHbaseTable, scan, HBaseCopyMap.class, ImmutableBytesWritable.class, Result.class, job);
        TableMapReduceUtil.initTableReducerJob(dstHbaseTable, null, job);

        return job;
    }

    public int run(String[] args) throws Exception {
        Configuration conf = RhinoETLConfig.getInstance();
        Job job = createSubmittableJob(conf, args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        new CopyTable().run(args);
    }

}
