package com.datastory.banyan.spark;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.hbase.HBaseUtils;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.serialize.FastJsonSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;

import java.io.IOException;
import java.io.Serializable;
import java.util.Random;

/**
 * com.datastory.banyan.spark.ScanFlushESMR
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class ScanFlushESMR implements Serializable {

    public int getReducerNum() {
        return 40;
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

    public Job buildJob(String table, Scan scan, Class<? extends TableMapper> mapperClass, Class<? extends Reducer> reducerClass) throws IOException {
        System.out.println("[SCAN] " + table + ";  " + scan);
        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        Configuration conf = job.getConfiguration();

        job.setJarByClass(this.getClass());
        job.setInputFormatClass(TableInputFormat.class);
        job.setJobName(this.getClass().getSimpleName() + "-" + table);
        TableMapReduceUtil.initTableMapperJob(table, scan, mapperClass, Text.class, Params.class, job);

        if (reducerClass != null)
            job.setReducerClass(reducerClass);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        if (reducerClass != null)
            job.setNumReduceTasks(getReducerNum());
        else
            job.setNumReduceTasks(0);
        conf.set("hbase.client.keyvalue.maxsize", "" + (50 * 1024 * 1024));
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapred.reduce.slowstart.completed.maps", "1.0");  // map跑完100% 才跑reducer
        conf.set("mapreduce.job.running.map.limit", "200");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.map.memory.mb", "2048");

//        conf.set("mapred.job.reuse.jvm.num.tasks", "1");
        return job;
    }

    /**
     * Mapper
     */
    public static abstract class ScanMapper extends TableMapper<Text, Params> {
        protected static Murmur3HashFunction hashFunc = new Murmur3HashFunction();
        static Random random = new Random();

        protected String routing(String pk) {
            String id = pk.substring(3);
            int i = hashFunc.hash(id);
            return i % 50 + "";
        }

        public abstract Params mapDoc(Params hbDoc);

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            context.getCounter(ROW.READ).increment(1);

            if (value == null || value.isEmpty()) {
                context.getCounter(ROW.FILTER).increment(1);
                return;
            }
            String pk = new String(value.getRow());
            if (pk.length() < 4) {
                context.getCounter(ROW.FILTER).increment(1);
                return;
            }

            String taskId = HBaseUtils.getValue(value, "m".getBytes(), "taskId".getBytes());
            Params hbDoc = new ResultRDocMapper(value).map();
            if (taskId != null)
                hbDoc.put("taskId", taskId);

            Params esDoc = mapDoc(hbDoc);

            if (esDoc != null) {
                String prefix = pk.substring(0, 3);
                context.write(new Text(prefix), esDoc);
                context.getCounter(ROW.SHUFFLE).increment(1);
            } else {
                context.getCounter(ROW.ERROR).increment(1);
                System.err.println("[ErrHbDoc] " + hbDoc);
            }
        }
    }

    /**
     * Reducer
     */
    public static abstract class FlushESReducer extends Reducer<Text, Params, NullWritable, NullWritable> {
        protected static Logger LOG = Logger.getLogger(FlushESReducer.class);
        protected long startTime;

        static {
//            RhinoETLConfig.getInstance().set("banyan.es.hooks.enable", "false");
            RhinoETLConsts.MAX_IMPORT_RETRY = 1;
        }

        public boolean isDebug() {
            return false;
        }

        public abstract ESWriterAPI getESWriter();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            startTime = System.currentTimeMillis();
            System.out.println("[TIME] start " + startTime);
        }

        @Override
        protected void reduce(Text key, Iterable<Params> values, final Context context) throws IOException, InterruptedException {
            ESWriter writer = (ESWriter) getESWriter();
            long size = 0;
            for (Params p : values) {
                size++;

                try {
                    if (!isDebug()) {
                        writer.write(p);
                        if (writer.getCurrentSize() > writer.getBulkNum() - 2) {
                            writer.awaitFlush();
                        }
                        context.getCounter(ROW.WRITE).increment(1);
                    } else
                        LOG.info("[DEBUG] " + FastJsonSerializer.serialize(p));
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
            System.out.println("[DEBUG] Total size : " + size);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!isDebug()) {
                ESWriter writer = (ESWriter) getESWriter();
                writer.awaitFlush();
//                writer.close();
            }
            long endTime = System.currentTimeMillis();
            System.out.println("[Time] end " + endTime + " , cost " + (endTime - startTime));
        }
    }

    public enum ROW {
        READ, WRITE, FILTER, PASS, ERROR, OTHER,
        MAP, REDUCE, LIST, SHUFFLE, ROWS, EMPTY, NULL,
        SIZE, DELETE, FAIL, SWITCH, DONE, NO_ID, NO_NAME,
        MAP_READ, MAP_WRITE, REDUCE_READ, REDUCE_WRITE,
        ES, HBASE, FILES, DIRS, RECORD
    }
}
