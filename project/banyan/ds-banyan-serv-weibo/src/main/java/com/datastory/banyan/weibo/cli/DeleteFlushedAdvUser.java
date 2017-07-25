package com.datastory.banyan.weibo.cli;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.HTableInterfacePool;
import com.datastory.banyan.spark.ScanFlushESMR;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueExcludeFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * com.datastory.banyan.weibo.cli.DeleteFlushedAdvUser
 *
 * @author lhfcws
 * @since 2017/4/11
 */
public class DeleteFlushedAdvUser {
    public static void main(String[] args) throws Exception {
        SingleColumnValueExcludeFilter filter = new SingleColumnValueExcludeFilter(
                "b".getBytes(), "is_flush".getBytes(),
                CompareFilter.CompareOp.GREATER_OR_EQUAL, "1".getBytes()
        );
        DeleteFlushedAdvUser runner = new DeleteFlushedAdvUser();
        Scan scan = runner.buildAllScan(filter);

        Job job = runner.buildJob(Tables.table(Tables.PH_WBADVUSER_TBL), scan, DeleteMapper.class);
        job.waitForCompletion(true);
    }

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

        job.setJarByClass(this.getClass());
        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setJobName(this.getClass().getSimpleName() + "-" + table);
        TableMapReduceUtil.initTableMapperJob(table, scan, mapperClass, NullWritable.class, NullWritable.class, job);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.setNumReduceTasks(0);
        conf.set("hbase.client.keyvalue.maxsize", "" + (50 * 1024 * 1024));
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapred.reduce.slowstart.completed.maps", "1.0");  // map跑完100% 才跑reducer
        conf.set("mapreduce.job.running.map.limit", "100");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.map.memory.mb", "2048");

//        conf.set("mapred.job.reuse.jvm.num.tasks", "1");
        return job;
    }

    public static class DeleteMapper extends TableMapper<NullWritable, NullWritable> {
        public static final int MAX_DELETE_SIZE = 1000;
        private List<Delete> deletes = new LinkedList<>();

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            Delete delete = new Delete(value.getRow());
            deletes.add(delete);

            if (deletes.size() > MAX_DELETE_SIZE) {
                cleanup(context);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!deletes.isEmpty()) {
                HTableInterface hti = HTableInterfacePool.get(Tables.table(Tables.PH_WBADVUSER_TBL));
                try {
                    hti.delete(deletes);
                    context.getCounter(ScanFlushESMR.ROW.DELETE).increment(deletes.size());
                } finally {
                    hti.flushCommits();
                    hti.close();
                }
                deletes.clear();
            }
        }
    }
}
