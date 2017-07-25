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

public class CountUpdateDateHbaseTableMR implements Serializable {

    static final byte[] R = "r".getBytes();

    public void run(String table) throws Exception {

        Scan scan = HBaseUtils.buildScan();
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
            String updateDate = HBaseUtils.getValue(result, R, "update_date".getBytes());
            String catId = HBaseUtils.getValue(result, R, "cat_id".getBytes());
            if (StringUtils.isNotEmpty(updateDate)) {
                context.getCounter(ROW.HAVE_UPDATE_TIME).increment(1);
            } else {
                context.getCounter(ROW.NO_UPDATE_TIME).increment(1);
            }
            if (StringUtils.isNotEmpty(catId)) {
                if (catId.trim().equalsIgnoreCase("2")) {
                    context.getCounter(ROW.CAT_ID_2).increment(1);
                } else if (catId.trim().equalsIgnoreCase("3")) {
                    context.getCounter(ROW.CAT_ID_3).increment(1);
                }
            } else {
                context.getCounter(ROW.NO_CAT).increment(1);
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
        new CountUpdateDateHbaseTableMR().run(table);
        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }

    public enum ROW {
        READ, HAVE_UPDATE_TIME, NO_CAT, CAT_ID_2, CAT_ID_3, NO_UPDATE_TIME, WRITE, T_SIZE, S_SIZE, CONTAIN_T, CONTAIN_S, BOTHNOCONTAIN, BOTHEMPTY, FILTER, PASS, ERROR, OTHER,
        MAP, LIST, SHUFFLE, ROWS, EMPTY, NULL,
        SIZE, DELETE
    }
}
