package com.datastory.banyan.migrate1.tools;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.HTableInterfacePool;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
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
import java.util.LinkedList;
import java.util.List;


/**
 * com.datastory.banyan.migrate1.tools.FixLTPPcOnlineCatIdMR
 *
 * @author abel.chan
 * @since 17/06/19
 */

public class DeleteNoUpdateDateHbaseTableMR implements Serializable {

    static final byte[] R = "r".getBytes();

    public void run(String table) throws Exception {

        Scan scan = HBaseUtils.buildScan();
        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        Configuration conf = job.getConfiguration();
        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setJobName(this.getClass().getSimpleName() + "-" + table);
        TableMapReduceUtil.initTableMapperJob(table, scan, DeleteMapper.class, NullWritable.class, NullWritable.class, job);
//        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJarByClass(this.getClass());
//        job.setNumReduceTasks(getReducerNum());
        job.setNumReduceTasks(0);

        //将表名作为全局参数传入。
        conf.set("banyan.hbase.table", table);

        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.job.user.classpath.first", "true");
//        conf.set("mapred.reduce.slowstart.completed.maps", "0.8");  // map跑完80% 才跑reducer
        conf.set("mapreduce.job.running.map.limit", "300");
        job.waitForCompletion(true);
    }

    public static class DeleteMapper extends TableMapper<NullWritable, NullWritable> {

        private String tableName;
        public static final int MAX_DELETE_SIZE = 1000;
        private List<Delete> deletes = new LinkedList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            tableName = context.getConfiguration().get("banyan.hbase.table");
            if (StringUtils.isEmpty(tableName)) {
                throw new InterruptedException("tableName is not null!");
            }
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            context.getCounter(ROW.READ).increment(1);
            String updateDate = HBaseUtils.getValue(result, R, "update_date".getBytes());
            if (StringUtils.isNotEmpty(updateDate)) {
                context.getCounter(ROW.HAVE_UPDATE_TIME).increment(1);
            } else {
                context.getCounter(ROW.NO_UPDATE_TIME).increment(1);
                Delete delete = new Delete(result.getRow());
                deletes.add(delete);
                if (deletes.size() > MAX_DELETE_SIZE) {
                    cleanup(context);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!deletes.isEmpty()) {
                HTableInterface hti = HTableInterfacePool.get(tableName);
                int size = deletes.size();
                try {
                    hti.delete(deletes);
                    context.getCounter(ROW.DELETE).increment(size);
                } finally {
                    hti.flushCommits();
                    hti.close();
                }
                deletes.clear();
            }
        }
    }

    public static void main(String[] args) throws Exception {

        if (args == null || args.length != 1) {
            System.out.println("please input table name");
            System.exit(1);
        }

        String table = null;
        if (args != null && args[0].equalsIgnoreCase("post")) {
            table = Tables.table(Tables.PH_LONGTEXT_POST_TBL);
        } else if (args != null && args[0].equalsIgnoreCase("comment")) {
            table = Tables.table(Tables.PH_LONGTEXT_CMT_TBL);
        } else {
            System.out.println("parameter is not illegal！");
            System.exit(1);
        }

        if (StringUtils.isEmpty(table)) {
            System.out.println("table name is not right!" + table);
            System.exit(1);
        }

        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());
        new DeleteNoUpdateDateHbaseTableMR().run(table);
        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }

    public enum ROW {
        READ, HAVE_UPDATE_TIME, NO_UPDATE_TIME, WRITE, T_SIZE, S_SIZE, CONTAIN_T, CONTAIN_S, BOTHNOCONTAIN, BOTHEMPTY, FILTER, PASS, ERROR, OTHER,
        MAP, LIST, SHUFFLE, ROWS, EMPTY, NULL,
        SIZE, DELETE
    }
}
