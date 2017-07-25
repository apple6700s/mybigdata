package com.datastory.banyan.migrate1.tools;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.spark.ScanFlushESMR;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

/**
 * com.datastory.banyan.migrate1.tools.FixTaskId
 *
 * @author xiangmin
 * @since 2017/5/7
 */
public class FixTaskId {
    static byte[] R = "r".getBytes();
    static byte[] M = "m".getBytes();

    public Scan buildScan() {
        Scan scan = HBaseUtils.buildScan();
//        SingleColumnValueFilter filter = new SingleColumnValueFilter(R, "taskId".getBytes(), CompareFilter.CompareOp.NOT_EQUAL, new NullComparator());
//        scan.setFilter(filter);
        scan.addColumn(R, "taskId".getBytes());
        scan.addColumn(M, "taskId".getBytes());
        return scan;
    }

    public Job createJob(Scan scan, String table) throws IOException {
        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        Configuration conf = job.getConfiguration();

        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setJobName(this.getClass().getSimpleName() + "-" + table);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJarByClass(this.getClass());
        job.setNumReduceTasks(0);

        conf.set("banyan.table", table);
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapreduce.job.running.map.limit", "200");

        TableMapReduceUtil.initTableMapperJob(table, scan, FixMapper.class, NullWritable.class, NullWritable.class, job);
        return job;
    }

    public static class FixMapper extends TableMapper<Text, Text> {
        RFieldPutter putter;
        private String table;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            table = context.getConfiguration().get("banyan.table");
            System.out.println("hbase table: " + table);
            if (StringUtils.isEmpty(table))
                throw new InterruptedException("table is null");
            putter = new RFieldPutter(table);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            context.getCounter(ScanFlushESMR.ROW.ROWS).increment(1);
            String pk = new String(result.getRow());
            String taskId = HBaseUtils.getValue(result, R, "taskId".getBytes());
            String taskId2 = HBaseUtils.getValue(result, M, "taskId".getBytes());

            if (StringUtils.isNotEmpty(taskId)) {
//                Put put = new Put(pk.getBytes());
//                put.addColumn(M, "taskId".getBytes(), taskId.getBytes());
//                putter.batchWrite(put);
                context.getCounter(ScanFlushESMR.ROW.WRITE).increment(1);
            }

            if (StringUtils.isNotEmpty((taskId2))) {
                context.getCounter(ScanFlushESMR.ROW.PASS).increment(1);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            putter.flush();
        }
    }

    public static void main(String[] args) throws Exception {
        FixTaskId runner = new FixTaskId();
        Scan scan = runner.buildScan();
        Job job;

        job = runner.createJob(scan, Tables.table(Tables.PH_LONGTEXT_POST_TBL));
        job.waitForCompletion(true);
        System.out.println("============================================");
        job = runner.createJob(scan, Tables.table(Tables.PH_LONGTEXT_CMT_TBL));
        job.waitForCompletion(true);
        System.out.println("[PROGRAM] Program exited.");
    }

}
