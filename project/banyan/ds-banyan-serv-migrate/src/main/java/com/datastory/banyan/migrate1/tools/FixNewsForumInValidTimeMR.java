package com.datastory.banyan.migrate1.tools;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.weibo.abel.Tables;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * com.datastory.banyan.migrate1.tools.FixLTPPcOnlineCatIdMR
 *
 * @author abel.chan
 * @since 17/06/19
 */

public class FixNewsForumInValidTimeMR implements Serializable {

    static Log log = LogFactory.getLog(FixNewsForumInValidTimeMR.class);

    static String table = Tables.table(Tables.PH_LONGTEXT_POST_TBL);

    static final byte[] R = "r".getBytes();

    public Job buildJob(String table) throws IOException {

        Scan scan = HBaseUtils.buildScan();
        scan.addColumn(R, "publish_date".getBytes());
        scan.addColumn(R, "source".getBytes());

        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        Configuration conf = job.getConfiguration();
        conf.set("banyan.hbase.table", table);

        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setJobName(this.getClass().getSimpleName() + "-" + table);
        TableMapReduceUtil.initTableMapperJob(table, scan, FixMapper.class, NullWritable.class, NullWritable.class, job);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJarByClass(this.getClass());
        job.setNumReduceTasks(0);
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.job.user.classpath.first", "true");
//        conf.set("mapred.reduce.slowstart.completed.maps", "0.8");  // map跑完80% 才跑reducer
        conf.set("mapreduce.job.running.map.limit", "300");
        return job;
    }

    public void run(String table) throws Exception {
        Job job = buildJob(table);
        job.waitForCompletion(true);
    }

    public static class FixMapper extends TableMapper<NullWritable, NullWritable> {
        RFieldPutter putter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            String table = context.getConfiguration().get("banyan.hbase.table");
            putter = new RFieldPutter(table);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            context.getCounter(ROW.READ).increment(1);

            String publishDate = HBaseUtils.getValue(result, R, "publish_date".getBytes());
            String source = HBaseUtils.getValue(result, R, "source".getBytes());
            SimpleDateFormat sdf = new SimpleDateFormat(DateUtils.DFT_TIMEFORMAT);

            boolean isTimeReset = false;
            boolean isSourceReset = false;

            try {
                if (StringUtils.isEmpty(publishDate)) {
                    isTimeReset = true;
                    context.getCounter(ROW.TIME_NULL).increment(1);
                } else {
                    sdf.parse(publishDate);
                    context.getCounter(ROW.TIME_WRITE).increment(1);
                }
            } catch (ParseException e) {
                log.error("[time][pk]:" + new String(result.getRow()) + ",[publish_date]:" + publishDate);
                isTimeReset = true;
                context.getCounter(ROW.TIME_ERROR).increment(1);
            }

            try {
                if (StringUtils.isEmpty(source)) {
                    isSourceReset = true;
                    context.getCounter(ROW.SOURCE_NULL).increment(1);
                } else if (source.trim().equalsIgnoreCase("{source}")) {
                    isSourceReset = true;
                    context.getCounter(ROW.SOURCE_EROOR).increment(1);
                } else {
                    context.getCounter(ROW.SOURCE_WRITE).increment(1);
                }
            } catch (Exception e) {
                log.error("[source][pk]:" + new String(result.getRow()) + ",[source]:" + source);
                context.getCounter(ROW.ERROR).increment(1);
            }
            if (isTimeReset || isSourceReset) {
                Put put = new Put(result.getRow());
                if (isTimeReset) {
                    put.addColumn(R, "publish_date".getBytes(), null);
                }
                if (isSourceReset) {
                    put.addColumn(R, "source".getBytes(), null);
                }
                putter.batchWrite(put);
            }


        }

//        public boolean containKey(Map map, String key) {
//            if (StringUtils.isEmpty(key) || map == null) return false;
//            return map.containsKey(key);
//        }
//

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            putter.flush();
        }
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        if (args != null && args[0].equalsIgnoreCase("post")) {
            String table = Tables.table(Tables.PH_LONGTEXT_POST_TBL);
            new FixNewsForumInValidTimeMR().run(table);
        } else if (args != null && args[0].equalsIgnoreCase("comment")) {
            String table = Tables.table(Tables.PH_LONGTEXT_CMT_TBL);
            new FixNewsForumInValidTimeMR().run(table);
        } else {
            System.out.println("parameter is not illegal！");
        }

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new

                Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }

    public enum ROW {
        READ, TIME_WRITE, ERROR, SOURCE_WRITE, SOURCE_EROOR, TIME_ERROR, TIME_NULL, SOURCE_NULL
    }
}
