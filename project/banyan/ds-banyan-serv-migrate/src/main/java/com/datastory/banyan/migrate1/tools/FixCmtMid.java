package com.datastory.banyan.migrate1.tools;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.spark.ScanFlushESMR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

/**
 * com.datastory.banyan.migrate1.tools.FixCmtMid
 *
 * @author lhfcws
 * @since 2017/6/5
 */
public class FixCmtMid {
    static byte[] R = "r".getBytes();
    private static String table = Tables.table(Tables.PH_WBCMT_TBL);

    public Scan buildScan() {
        Scan scan = HBaseUtils.buildScan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter("r".getBytes(), "cmt_id".getBytes(), CompareFilter.CompareOp.NOT_EQUAL, new NullComparator());
        scan.setFilter(filter);
        scan.addColumn(R, "cmt_id".getBytes());
        return scan;
    }

    public Job createJob(Scan scan) throws IOException {
        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        Configuration conf = job.getConfiguration();

        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setJobName(this.getClass().getSimpleName() + "-" + table);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJarByClass(this.getClass());
        job.setNumReduceTasks(0);

        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapreduce.job.running.map.limit", "200");

        TableMapReduceUtil.initTableMapperJob(table, scan, FixMapper.class, NullWritable.class, NullWritable.class, job);
        return job;
    }

    public static class FixMapper extends TableMapper<Text, Text> {
        RFieldPutter putter;

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {
            System.out.println("hbase table: " + table);
            putter = new RFieldPutter(table);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Mapper.Context context) throws IOException, InterruptedException {
            context.getCounter(ScanFlushESMR.ROW.READ).increment(1);
            String pk = new String(result.getRow());
            String cmtId = HBaseUtils.getValue(result, R, "cmt_id".getBytes());
            if (cmtId == null)
                return;
            Put put = new Put(pk.getBytes());
            put.add(R, "cmt_mid".getBytes(), cmtId.getBytes());
            putter.batchWrite(put);
            context.getCounter(ScanFlushESMR.ROW.WRITE).increment(1);
        }

        @Override
        protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {
            putter.flush();
        }
    }


    public static void main(String[] args) throws Exception {
        FixCmtMid runner = new FixCmtMid();
        Scan scan = runner.buildScan();
        Job job = runner.createJob(scan);
        job.waitForCompletion(true);
        System.out.println("[PROGRAM] Program exited.");
    }
}
