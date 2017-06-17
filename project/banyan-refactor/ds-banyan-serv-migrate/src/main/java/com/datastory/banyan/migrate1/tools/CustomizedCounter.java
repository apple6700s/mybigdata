package com.datastory.banyan.migrate1.tools;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.datastory.banyan.utils.BanyanTypeUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.lang.management.ManagementFactory;

/**
 * com.datastory.banyan.migrate1.tools.CustomizedCounter
 *
 * @author lhfcws
 * @since 2017/2/27
 */
public class CustomizedCounter {
    static byte[] R = "r".getBytes();

    public Scan buildScan() {
        Scan scan = HBaseUtils.buildScan();
        FilterList filterList = new FilterList();
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter("r".getBytes(), "src_content".getBytes(),  CompareFilter.CompareOp.EQUAL, new NullComparator());
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter("r".getBytes(), "src_mid".getBytes(),  CompareFilter.CompareOp.NOT_EQUAL, new NullComparator());
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);

        scan.setFilter(filterList);

        scan.addColumn(R, "src_content".getBytes());
        scan.addColumn(R, "src_mid".getBytes());
        return scan;
    }

    public Job createJob(Scan scan, String table) throws IOException {
        Configuration conf = RhinoETLConfig.create();
        Job job = Job.getInstance(conf);
        conf = job.getConfiguration();
//        conf.set("hbase.client.keyvalue.maxsize", "" + (50 * 1024 * 1024));
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapred.reduce.slowstart.completed.maps", "1.0");  // map跑完100% 才跑reducer
        conf.set("mapreduce.job.running.map.limit", "200");
//        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.map.memory.mb", "1024");

        job.setJobName(this.getClass().getSimpleName() + "-" + table);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setJarByClass(this.getClass());
        job.setNumReduceTasks(0);
        TableMapReduceUtil.initTableMapperJob(table, scan, RowCounter.class, NullWritable.class, NullWritable.class, job);

        return job;
    }

    public static class RowCounter extends TableMapper<NullWritable, NullWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            if (result != null && !result.isEmpty()) {
                context.getCounter(ScanFlushESMR.ROW.ROWS).increment(1);
                String srcMid = HBaseUtils.getValue(result, R, "src_mid".getBytes());
                if (BanyanTypeUtil.valid(srcMid) && srcMid.length() > 10) {
                    String srcContent = HBaseUtils.getValue(result, R, "src_content".getBytes());
                    if (srcContent == null)
                        context.getCounter(ScanFlushESMR.ROW.FILTER).increment(1);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        CustomizedCounter counter = new CustomizedCounter();
        Job job = counter.createJob(counter.buildScan(), "DS_BANYAN_WEIBO_CONTENT_V1");
        job.waitForCompletion(true);
        System.out.println("[PROGRAM] Program exited.");
    }
}
