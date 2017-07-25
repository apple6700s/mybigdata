package com.datastory.banyan.migrate1.tools;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Random;

/**
 * com.datastory.banyan.migrate1.tools.RowCounter
 */
public class RowCounter {
    public Scan buildAllScan(Filter filter) {
        Scan scan = HBaseUtils.buildScan();
        if (filter != null)
            scan.setFilter(filter);
        return scan;
    }

    public Scan buildAllScan() {
        return buildAllScan(null);
    }

    public Job buildJob(String table, Scan scan, Class<? extends TableMapper> mapperClass) throws IOException {
        System.out.println("[SCAN] " + table + ";  " + scan);
        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        Configuration conf = job.getConfiguration();

        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setJobName(this.getClass().getSimpleName() + "-" + table);
        TableMapReduceUtil.initTableMapperJob(table, scan, mapperClass, NullWritable.class, NullWritable.class, job);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJarByClass(this.getClass());

        job.setNumReduceTasks(0);
        conf.set("hbase.client.keyvalue.maxsize", "" + (50 * 1024 * 1024));
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapred.reduce.slowstart.completed.maps", "1.0");  // map跑完100% 才跑reducer
        conf.set("mapreduce.job.running.map.limit", "200");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.map.memory.mb", "2048");

//        conf.set("mapred.job.reuse.jvm.num.tasks", "1");
        return job;
    }

    /**
     * Mapper
     */
    public static abstract class ScanMapper extends TableMapper<NullWritable, NullWritable> {
        protected static Murmur3HashFunction hashFunc = new Murmur3HashFunction();
        static Random random = new Random();

        protected String routing(String pk) {
            String id = pk.substring(3);
            int i = hashFunc.hash(id);
            return i % 50 + "";
        }

        public abstract Params mapDoc(Params hbDoc);

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Mapper.Context context) throws IOException, InterruptedException {
            context.getCounter(ScanFlushESMR.ROW.READ).increment(1);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        RowCounter rowCounter = new RowCounter();
        Scan scan = rowCounter.buildAllScan();
        scan.addColumn("r".getBytes(), "mid".getBytes());
        Job job = rowCounter.buildJob(Tables.table(Tables.PH_WBCNT_TBL), scan, ScanMapper.class);
        job.waitForCompletion(true);
        System.out.println("[PROGRAM] Program exited.");
    }
}
