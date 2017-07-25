package com.datastory.banyan.migrate1.tools;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.weibo.abel.Tables;
import com.yeezhao.commons.util.ClassUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
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
import java.io.InputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * com.datastory.banyan.migrate1.tools.FixLTPPcOnlineCatIdMR
 *
 * @author abel.chan
 * @since 17/06/19
 */

public class FixLTPPcOnlineCatIdMR implements Serializable {

    static String table = Tables.table(Tables.PH_LONGTEXT_POST_TBL);

    static final byte[] R = "r".getBytes();

    public Job buildJob() throws IOException {

        Scan scan = HBaseUtils.buildScan();
//        SingleColumnValueFilter filter = new SingleColumnValueFilter(R, "taskId".getBytes(), CompareFilter.CompareOp.EQUAL, "116833".getBytes());
//
//        scan.setFilter(filter);
        scan.addColumn(R, "taskId".getBytes());
        scan.addColumn(R, "cat_id".getBytes());
        scan.addColumn(R, "site_id".getBytes());

        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        Configuration conf = job.getConfiguration();

//        try {
//            String hdfsPath = "hdfs://alps-cluster/tmp/taskid_to_catid.txt";
//            DistributedCache.addCacheFile(new URI(hdfsPath), conf);
//            hdfsPath = "hdfs://alps-cluster/tmp/site_cat.txt";
//            DistributedCache.addCacheFile(new URI(hdfsPath), conf);
//        } catch (URISyntaxException e) {
//
//        }


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
        return job;
    }

    public void run() throws Exception {
        Job job = buildJob();
        job.waitForCompletion(true);
    }

    public static class FixMapper extends TableMapper<NullWritable, NullWritable> {
        RFieldPutter putter;

        private Map<String, String> taskId2CatIdMapping = new HashMap<String, String>();
        private Map<String, String> siteId2CatIdMapping = new HashMap<String, String>();
//        private Path[] localFiles;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            putter = new RFieldPutter(table);
//            Path[] localFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
//            String taskId2CatIdPath = localFiles[0].toString();
//
//            BufferedReader bReader = new BufferedReader(new FileReader(
//                    taskId2CatIdPath));
//            InputStream inputStream = ClassUtil.getResourceAsInputStream("taskid_to_catid.txt");
//            List<String> values = IOUtils.readLines(inputStream);
//
//            for (String line : values) {
//                String[] split = line.split("\t");
//                if (split != null && split.length == 2) {
//                    taskId2CatIdMapping.put(split[0], split[1]);
//                }
//            }
            taskId2CatIdMapping = getFileData("taskid_to_catid.txt");
            context.getCounter(ROW.T_SIZE).setValue(taskId2CatIdMapping.size());
            System.out.println("[taskId2CatIdMapping]:" + taskId2CatIdMapping);
            siteId2CatIdMapping = getFileData("site_cat.txt");
            context.getCounter(ROW.S_SIZE).setValue(siteId2CatIdMapping.size());
            System.out.println("[siteId2CatIdMapping]:" + siteId2CatIdMapping);
        }

        public Map<String, String> getFileData(String filePath) {
            Map<String, String> map = new HashMap<String, String>();
            try {
                InputStream inputStream = ClassUtil.getResourceAsInputStream(filePath);
                List<String> values = IOUtils.readLines(inputStream);

                for (String line : values) {
                    String[] split = line.split("\t");
                    if (split != null && split.length == 2) {
                        map.put(split[0], split[1]);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            return map;
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            context.getCounter(ROW.READ).increment(1);
//            context.getCounter(ScanFlushESMR.ROW.WRITE).increment(taskId2CatIdMapping.size());
//            String catId = HBaseUtils.getValue(result, R, "cat_id".getBytes());

            String taskId = HBaseUtils.getValue(result, R, "taskId".getBytes());
            String siteId = HBaseUtils.getValue(result, R, "site_id".getBytes());
            String catId = HBaseUtils.getValue(result, R, "cat_id".getBytes());

            if (StringUtils.isNotEmpty(catId) && "3".equals(catId.trim())) {
                if (StringUtils.isEmpty(taskId) && StringUtils.isEmpty(siteId)) {
                    System.out.println(ROW.BOTHEMPTY + ":" + new String(result.getRow()));
                    context.getCounter(ROW.BOTHEMPTY).increment(1);
                } else if (!containKey(taskId2CatIdMapping, taskId) && !containKey(siteId2CatIdMapping, siteId)) {
                    System.out.println(ROW.BOTHNOCONTAIN + ":" + new String(result.getRow()));
                    context.getCounter(ROW.BOTHNOCONTAIN).increment(1);
                } else if (containKey(taskId2CatIdMapping, taskId)) {
                    //优先使用task_id
                    context.getCounter(ROW.CONTAIN_T).increment(1);
                } else if (containKey(siteId2CatIdMapping, siteId)) {
                    //说明taskid不存在，再找site_id
                    context.getCounter(ROW.CONTAIN_S).increment(1);
                } else {
                    System.out.println(ROW.ERROR + ":" + new String(result.getRow()));
                    context.getCounter(ROW.ERROR).increment(1);
                }
            } else {
                context.getCounter(ROW.CAT_NO_EQUAL_3).increment(1);
            }

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

        public boolean containKey(Map map, String key) {
            if (StringUtils.isEmpty(key) || map == null) return false;
            return map.containsKey(key);
        }

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


        new FixLTPPcOnlineCatIdMR().run();

//        table = Tables.table(Tables.PH_LONGTEXT_CMT_TBL);
//
//        new FixLTPPcOnlineCatIdMR().run();


        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }

    public enum ROW {
        READ, WRITE, CAT_NO_EQUAL_3, T_SIZE, S_SIZE, CONTAIN_T, CONTAIN_S, BOTHNOCONTAIN, BOTHEMPTY, FILTER, PASS, ERROR, OTHER,
        MAP, LIST, SHUFFLE, ROWS, EMPTY, NULL,
        SIZE, DELETE
    }
}
