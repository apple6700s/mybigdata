package com.datastory.banyan.wechat.flush_es;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.datastory.banyan.utils.StrMapWritable;
import com.datastory.banyan.wechat.doc.WxCntHb2ESDocMapper;
import com.datastory.banyan.wechat.es.WxCntESWriter;
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
import java.lang.management.ManagementFactory;
import java.util.Random;


/**
 * com.datastory.banyan.wechat.flush_es.WechatContentFlushESMR
 *
 * @author lhfcws
 * @since 16/12/6
 */

public class WechatContentFlushESMR {
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

    public Job buildJob(String table, Scan scan, Class<? extends ScanMapper> mapperClass, Class<? extends Reducer> reducerClass) throws IOException {
        System.out.println("[SCAN] " + table + ";  " + scan);
        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        Configuration conf = job.getConfiguration();

        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setJobName(this.getClass().getSimpleName() + "-" + table);
        TableMapReduceUtil.initTableMapperJob(table, scan, mapperClass, Text.class, StrMapWritable.class, job);
        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJarByClass(this.getClass());

        job.setNumReduceTasks(getReducerNum());
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
    public static class ScanMapper extends TableMapper<Text, StrMapWritable> {
        protected static Murmur3HashFunction hashFunc = new Murmur3HashFunction();
        static Random random = new Random();

        protected String routing(String pk) {
//            String id = pk.substring(3);
//            int i = hashFunc.hash(id);
//            return i + "";
//            return random.nextInt(1000) + "";
            return pk.substring(0, 2);
        }

        public Params mapDoc(Params hbDoc) {
            return new WxCntHb2ESDocMapper(hbDoc).map();
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            context.getCounter(ScanFlushESMR.ROW.READ).increment(1);

            if (value == null || value.isEmpty()) {
                context.getCounter(ScanFlushESMR.ROW.FILTER).increment(1);
                return;
            }
            String pk = new String(value.getRow());
            if (pk.length() < 4) {
                context.getCounter(ScanFlushESMR.ROW.FILTER).increment(1);
                return;
            }

            Params hbDoc = new ResultRDocMapper(value).map();
            Params esDoc = mapDoc(hbDoc);

            if (esDoc != null) {
                String prefix = routing(pk);
                context.write(new Text(prefix), new StrMapWritable(esDoc));
                context.getCounter(ScanFlushESMR.ROW.SHUFFLE).increment(1);
            } else {
                context.getCounter(ScanFlushESMR.ROW.ERROR).increment(1);
                System.err.println("[ErrHbDoc] " + hbDoc);
            }
        }
    }

    /**
     * Reducer
     */
    public static class FlushESReducer extends Reducer<Text, StrMapWritable, NullWritable, NullWritable> {
        protected static Logger LOG = Logger.getLogger(ScanFlushESMR.FlushESReducer.class);
        protected long startTime;
        protected ESWriter writer;


        static {
            RhinoETLConsts.MAX_IMPORT_RETRY = 1;
//            RhinoETLConfig.getInstance().set("banyan.es.hooks.enable", "false");
        }

        public boolean isDebug() {
            return false;
        }

        public ESWriterAPI getESWriter() {
            return WxCntESWriter.getFlushInstance();
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            if (!isDebug()) {
                writer = (ESWriter) getESWriter();
            }
            startTime = System.currentTimeMillis();
            System.out.println("[TIME] start " + startTime);
        }

        @Override
        protected void reduce(Text key, Iterable<StrMapWritable> values, Context context) throws IOException, InterruptedException {
            long size = 0;
            for (StrMapWritable v : values) {
                size++;
                Params p = v.toParams();

                try {
                    if (!isDebug()) {
                        writer.write(p);
                        if (writer.getCurrentSize() >= writer.getBulkNum() - 2) {
                            writer.awaitFlush();
                        }
                        context.getCounter(ScanFlushESMR.ROW.WRITE).increment(1);
                    }
                    LOG.info("[DEBUG] " + FastJsonSerializer.serialize(p));
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
//            System.out.println("[DEBUG] Total size : " + size);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!isDebug()) {
                writer.awaitFlush();
//                writer.close();
            }
            long endTime = System.currentTimeMillis();
            System.out.println("[Time] end " + endTime + " , cost " + (endTime - startTime));
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        WechatContentFlushESMR runner = new WechatContentFlushESMR();
        Scan scan = runner.buildAllScan();
        Job job = runner.buildJob(Tables.table(Tables.PH_WXCNT_TBL), scan, ScanMapper.class, FlushESReducer.class);
        job.waitForCompletion(true);
        System.out.println("[PROGRAM] Program exited.");
    }
}
