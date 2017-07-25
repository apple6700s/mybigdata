package com.datastory.banyan.migrate1.tools;

import com.datastory.banyan.analyz.CommonLongTextAnalyzer;
import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.weibo.analyz.WbContentAnalyzer;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

/**
 * com.datastory.banyan.migrate1.tools.FixEscapeContent
 *
 * @author lhfcws
 * @since 2017/3/14
 */
public class FixEscapeContent {
    public Scan buildAllScan(Filter filter) {
        Scan scan = HBaseUtils.buildScan();
        if (filter != null)
            scan.setFilter(filter);
        return scan;
    }

    public Scan buildAllScan() {
        return buildAllScan(null);
    }

    public Job buildJob(String table, Scan scan) throws IOException {
        System.out.println("[SCAN] " + table + ";  " + scan);
        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        Configuration conf = job.getConfiguration();

        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setJobName(this.getClass().getSimpleName() + "-" + table);
        TableMapReduceUtil.initTableMapperJob(table, scan, FixMapper.class, NullWritable.class, NullWritable.class, job);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJarByClass(this.getClass());

        job.setNumReduceTasks(0);
        conf.set("banyan.table", table);
        conf.set("hbase.client.keyvalue.maxsize", "" + (50 * 1024 * 1024));
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapred.reduce.slowstart.completed.maps", "1.0");  // map跑完100% 才跑reducer
        conf.set("mapreduce.job.running.map.limit", "256");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.map.memory.mb", "2048");

//        conf.set("mapred.job.reuse.jvm.num.tasks", "1");
        return job;
    }

    public static void main(String[] args) throws Exception {
        new FixEscapeContent().run(args[0]);
        System.out.println("[PROGRAM] Program exited.");
    }

    public void run(String table) throws Exception {
        SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("r".getBytes(), "update_date".getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, "20170305000000".getBytes());
        Scan scan = buildAllScan(singleColumnValueFilter);
        scan.addColumn("r".getBytes(), "content".getBytes());
        scan.addColumn("r".getBytes(), "update_date".getBytes());
        if (table.contains("DS_BANYAN_NEWSFORUM_POST"))
            scan.addColumn("r".getBytes(), "all_content".getBytes());

        Job job = buildJob(table, scan);
        job.waitForCompletion(true);
    }

    public static class FixMapper extends TableMapper<NullWritable, NullWritable> {
        protected int contentMode = 0;
        protected RFieldPutter putter;
        protected CommonLongTextAnalyzer analyzer;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String table = context.getConfiguration().get("banyan.table");
            if (table.contains("DS_BANYAN_NEWSFORUM_POST"))
                contentMode = 1;
            else if (table.contains("WEIBO"))
                contentMode = 2;
            putter = new RFieldPutter(table);
            if (contentMode != 2) {
                analyzer = new CommonLongTextAnalyzer();
            } else {
                WbContentAnalyzer.getInstance();
            }
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            Params p = new ResultRDocMapper(result).map();
            context.getCounter(ScanFlushESMR.ROW.READ).increment(1);
            if (contentMode == 2) {
                p = WbContentAnalyzer.getInstance().analyz(p);
                putter.batchWrite(p);
            } else {
                try {
                    unescape(p, "content");
                    if (contentMode == 1) {
                        unescape(p, "all_content");
                    }
                    p = analyzer.analyz(p);
                    putter.batchWrite(p);
                } catch (Exception e) {
                    System.err.println(e.getMessage() + " - " + p);
                    context.getCounter(ScanFlushESMR.ROW.ERROR).increment(1);
                }
            }
        }

        protected void unescape(Params p, String key) throws Exception {
            String s = p.getString(key);
            if (BanyanTypeUtil.valid(s)) {
                s = StringEscapeUtils.unescapeJava(s);
                p.put(key, s);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            putter.flush();
        }
    }
}
