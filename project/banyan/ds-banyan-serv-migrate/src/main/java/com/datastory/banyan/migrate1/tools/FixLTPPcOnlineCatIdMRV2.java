package com.datastory.banyan.migrate1.tools;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.weibo.abel.Tables;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
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

public class FixLTPPcOnlineCatIdMRV2 implements Serializable {

    static String table = Tables.table(Tables.PH_LONGTEXT_POST_TBL);

    static final byte[] R = "r".getBytes();

    static final String PcOnlineTaskId = "116833";

    public Job buildJob(String table) throws IOException {

        Scan scan = HBaseUtils.buildScan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter(R, "taskId".getBytes(), CompareFilter.CompareOp.EQUAL, PcOnlineTaskId.getBytes());
//
        scan.setFilter(filter);
        scan.addColumn(R, "taskId".getBytes());
        scan.addColumn(R, "cat_id".getBytes());
        scan.addColumn(R, "site_id".getBytes());

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

            String taskId = HBaseUtils.getValue(result, R, "taskId".getBytes());
//            String catId = HBaseUtils.getValue(result, R, "cat_id".getBytes());

            if (StringUtils.isNotEmpty(taskId) && taskId.trim().equals(PcOnlineTaskId)) {
                putData(result.getRow(), "3".getBytes());
                context.getCounter(ROW.WRITE).increment(1);
            }

//            if (StringUtils.isEmpty(taskId)) {
//                context.getCounter(ROW.NO_T_ID).increment(1);
//            } else if (!taskId.trim().equals(PcOnlineTaskId)) {
//                context.getCounter(ROW.T_NOT_RIGHT).increment(1);
//            } else if (taskId.trim().equals(PcOnlineTaskId)) {
//                context.getCounter(ROW.T_COUNT).increment(1);
//            }
//
//            if (StringUtils.isNotEmpty(catId)) {
//                if (catId.trim().equalsIgnoreCase("2")) {
//                    context.getCounter(ROW.CAT_ID_2).increment(1);
//                } else if (catId.trim().equalsIgnoreCase("3")) {
//                    context.getCounter(ROW.CAT_ID_3).increment(1);
//                }
//            } else {
//                context.getCounter(ROW.NO_CAT).increment(1);
//            }

//            putter.flush();

//            if (StringUtils.isNotEmpty(taskId) && taskId2CatIdMapping.containsKey(taskId)) {
////                byte[] pk = result.getRow();
////                Put put = new Put(pk);
////                put.addColumn(R, "cat_id".getBytes(), taskId2CatIdMapping.get(taskId).getBytes());
////                putter.batchWrite(put);
//                context.getCounter(ScanFlushESMR.ROW.WRITE).increment(1);
//
//            } else if (StringUtils.isEmpty(taskId)) {
//                context.getCounter(ROW.EMPTY).increment(1);
//            } else {
//                context.getCounter(ROW.NOCONTAIN).increment(1);
//            }

//            if (StringUtils.isNotEmpty(catId) && catId.equals("2")) {
//                byte[] pk = result.getRow();
//                Put put = new Put(pk);
//                put.addColumn(R, "cat_id".getBytes(), "3".getBytes());
//
//                context.getCounter(ScanFlushESMR.ROW.WRITE).increment(1);
//                putter.batchWrite(put);
//            } else {
//                context.getCounter(ScanFlushESMR.ROW.ERROR).increment(1);
//            }
        }

//        public boolean containKey(Map map, String key) {
//            if (StringUtils.isEmpty(key) || map == null) return false;
//            return map.containsKey(key);
//        }
//

        public void putData(byte[] pk, byte[] catId) throws IOException {
            Put put = new Put(pk);
            put.addColumn(R, "cat_id".getBytes(), catId);
            putter.batchWrite(put);
        }

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
            new FixLTPPcOnlineCatIdMRV2().run(table);
        } else if (args != null && args[0].equalsIgnoreCase("comment")) {
            String table = Tables.table(Tables.PH_LONGTEXT_CMT_TBL);
            new FixLTPPcOnlineCatIdMRV2().run(table);
        } else {
            System.out.println("parameter is not illegal！");
        }

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new

                Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }

    public enum ROW {
        READ, NO_T_ID, T_NOT_RIGHT, T_COUNT, NO_CAT, CAT_ID_2, CAT_ID_3, WRITE, CAT_NO_EQUAL_3, T_SIZE, S_SIZE, CONTAIN_T, CONTAIN_S, BOTHNOCONTAIN, BOTHEMPTY, FILTER, PASS, ERROR, OTHER,
        MAP, LIST, SHUFFLE, ROWS, EMPTY, NULL,
        SIZE, DELETE
    }
}
