package com.datastory.banyan.migrate1.tools;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.hbase.HBaseUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
 * com.datastory.banyan.migrate1.tools.FixLTPPcOnlineCatIdMR
 *
 * @author abel.chan
 * @since 17/06/19
 */

public class CountWeiboCntHbaseTableMR implements Serializable {

    static final byte[] R = "r".getBytes();

    public void run(String table) throws Exception {

        Scan scan = HBaseUtils.buildScan();
        scan.addColumn("r".getBytes(), "reposts_cnt".getBytes());
        scan.addColumn("r".getBytes(), "attitudes_cnt".getBytes());
        scan.addColumn("r".getBytes(), "comments_cnt".getBytes());

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
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.job.user.classpath.first", "true");
//        conf.set("mapred.reduce.slowstart.completed.maps", "0.8");  // map跑完80% 才跑reducer
        conf.set("mapreduce.job.running.map.limit", "300");
        job.waitForCompletion(true);
    }

    public static class FixMapper extends TableMapper<NullWritable, NullWritable> {

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            context.getCounter(ROW.READ).increment(1);
            String repostsCnt = HBaseUtils.getValue(result, R, "reposts_cnt".getBytes());
            String attitudesCnt = HBaseUtils.getValue(result, R, "attitudes_cnt".getBytes());
            String commentsCnt = HBaseUtils.getValue(result, R, "comments_cnt".getBytes());
            if (StringUtils.isEmpty(repostsCnt)) {
                context.getCounter(ROW.REPOST_CNT_NULL).increment(1);
            } else {
                try {
                    int i = Integer.parseInt(repostsCnt);
                    if (i == 0) {
                        context.getCounter(ROW.REPOST_CNT_ZERO).increment(1);
                    } else {
                        context.getCounter(ROW.REPORT_CNT_NORMAL).increment(1);
                    }
                } catch (Exception ex) {
                    context.getCounter(ROW.REPORT_CNT_ERROR).increment(1);
                }

            }

            if (StringUtils.isEmpty(attitudesCnt)) {
                context.getCounter(ROW.ATTITUDE_CNT_NULL).increment(1);
            } else {
                try {
                    int i = Integer.parseInt(attitudesCnt);
                    if (i == 0) {
                        context.getCounter(ROW.ATTITUDE_CNT_ZERO).increment(1);
                    } else {
                        context.getCounter(ROW.ATTITUDE_CNT_NORMAL).increment(1);
                    }
                } catch (Exception ex) {
                    context.getCounter(ROW.ATTITUDE_CNT_ERROR).increment(1);
                }

            }

            if (StringUtils.isEmpty(commentsCnt)) {
                context.getCounter(ROW.COMMENT_CNT_NULL).increment(1);
            } else {
                try {
                    int i = Integer.parseInt(commentsCnt);
                    if (i == 0) {
                        context.getCounter(ROW.COMMENT_CNT_ZERO).increment(1);
                    } else {
                        context.getCounter(ROW.COMMENT_CNT_NORMAL).increment(1);
                    }
                } catch (Exception ex) {
                    context.getCounter(ROW.COMMENT_CNT_ERROR).increment(1);
                }

            }

        }

    }

    public static void main(String[] args) throws Exception {

        if (args == null || args.length != 1) {
            System.out.println("please input table name");
            System.exit(1);
        }
        String table = args[0];

        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());
        new CountWeiboCntHbaseTableMR().run(table);
        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }

    public enum ROW {
        READ, ERROR,
        REPOST_CNT_NULL, REPOST_CNT_ZERO, REPORT_CNT_NORMAL, REPORT_CNT_ERROR,
        ATTITUDE_CNT_NULL, ATTITUDE_CNT_ZERO, ATTITUDE_CNT_NORMAL, ATTITUDE_CNT_ERROR,
        COMMENT_CNT_NULL, COMMENT_CNT_ZERO, COMMENT_CNT_NORMAL, COMMENT_CNT_ERROR


    }
}
