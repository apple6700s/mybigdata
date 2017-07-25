package com.datastory.banyan.migrate1;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.weibo.analyz.MsgTypeAnalyzer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;


/**
 * com.datastory.banyan.migrate1.FixFieldMR
 *
 * @author lhfcws
 * @since 16/12/13
 */

public class FixFieldMR implements Serializable {
    static String table = "DS_BANYAN_WEIBO_CONTENT_V1";
    static final byte[] R = "r".getBytes();

    public Job buildJob() throws IOException {
        Scan scan = HBaseUtils.buildScan();
        scan.addColumn(R, "content".getBytes());
        scan.addColumn(R, "rt_mid".getBytes());
        scan.addColumn(R, "src_mid".getBytes());
        scan.addColumn(R, "self_content".getBytes());
        scan.addColumn(R, "msg_type".getBytes());

        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        Configuration conf = job.getConfiguration();

        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setJobName(this.getClass().getSimpleName() + "-" + table);
        TableMapReduceUtil.initTableMapperJob(table, scan, FixMapper.class, NullWritable.class, NullWritable.class, job);
//        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJarByClass(this.getClass());

//        job.setNumReduceTasks(getReducerNum());
        job.setNumReduceTasks(0);
        conf.set("mapreduce.job.user.classpath.first", "true");
//        conf.set("mapred.reduce.slowstart.completed.maps", "0.8");  // map跑完80% 才跑reducer
        conf.set("mapreduce.job.running.map.limit", "300");
        return job;
    }

    public void run() throws Exception {
        Job job = buildJob();
        job.waitForCompletion(true);
    }

    public static class FixMapper extends TableMapper<NullWritable, NullWritable> {
        RFieldPutter putter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            putter = new RFieldPutter(table);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            context.getCounter(ScanFlushESMR.ROW.READ).increment(1);
            String content = HBaseUtils.getValue(result, R, "content".getBytes());
            String rtMid = HBaseUtils.getValue(result, R, "rt_mid".getBytes());
            String srcMid = HBaseUtils.getValue(result, R, "src_mid".getBytes());
            String selfContent = HBaseUtils.getValue(result, R, "self_content".getBytes());
            String oldMsgType = HBaseUtils.getValue(result, R, "msg_type".getBytes());

            String msgType = "" + MsgTypeAnalyzer.analyz(srcMid, rtMid, selfContent, content);
            if (!msgType.equals(oldMsgType)) {
                byte[] pk = result.getRow();
                Put put = new Put(pk);
                put.addColumn(R, "msg_type".getBytes(), msgType.getBytes());

                context.getCounter(ScanFlushESMR.ROW.WRITE).increment(1);
                putter.batchWrite(put);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
           putter.flush();
        }
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        new FixFieldMR().run();

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
