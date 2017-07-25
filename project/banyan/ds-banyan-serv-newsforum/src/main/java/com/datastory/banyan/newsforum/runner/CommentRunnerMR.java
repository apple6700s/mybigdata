package com.datastory.banyan.newsforum.runner;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.validate.engine.ValidEngine;
import com.datastory.banyan.validate.engine.impl.XmlValidEngine;
import com.datastory.banyan.validate.entity.StatsResult;
import com.datastory.banyan.validate.entity.ValidResult;
import com.datastory.banyan.validate.stats.Stats;
import com.datastory.banyan.validate.stats.StatsFactory;
import com.yeezhao.commons.util.ClassUtil;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import java.io.InputStream;
import java.io.Serializable;

/**
 * Created by abel.chan on 17/7/18.
 */
public class CommentRunnerMR implements Serializable {
    static final byte[] R = "r".getBytes();

    static Log LOG = LogFactory.getLog(CommentRunnerMR.class);

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


        Stats stats;
        ValidEngine engine;
        String projectName;
        StatsResult statsResult;
        int maxValidResultSize;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            statsResult = new StatsResult();
            projectName = "comment_content";
            maxValidResultSize = 100;

            InputStream inputStream = ClassUtil.getResourceAsInputStream("newsforum-valid-rule-conf.xml");
            try {
                stats = StatsFactory.getStatsInstance();
                engine = XmlValidEngine.getInstance(inputStream);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
            if (engine == null) {
                throw new IOException("init valid engine failed !!!");
            }
            if (stats == null) {
                throw new InterruptedException("init stats obj failed !!!");
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            long start = System.currentTimeMillis();
            stats.write(projectName, statsResult);
            long cost = (System.currentTimeMillis() - start);
            context.getCounter(ROW.WRITE_REDIS_TIME).increment(cost);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            context.getCounter(ROW.READ).increment(1);

            try {
                long start = System.currentTimeMillis();
                Params map = new ResultRDocMapper(result).map();
                long cost = (System.currentTimeMillis() - start);
                context.getCounter(ROW.PARSE_TIME).increment(cost);
                LOG.info("[Params]:" + map.toJson());

                start = System.currentTimeMillis();
                ValidResult validResult = engine.execute(map, "comment");
                cost = (System.currentTimeMillis() - start);
                context.getCounter(ROW.VALID_TIME).increment(cost);
                LOG.info("[VALIDREDULT]:" + validResult.toString());
                start = System.currentTimeMillis();
                statsResult.add(validResult);
                cost = (System.currentTimeMillis() - start);
                context.getCounter(ROW.STATS_TIME).increment(cost);
                LOG.info("[STATSRESULT]:" + validResult.toString());

            } catch (Exception e) {
                context.getCounter(ROW.ERROR).increment(1);
                LOG.error(e.getMessage(), e);
            }
        }

    }

    public enum ROW {
        READ, WRITE, ERROR, PARSE_TIME, VALID_TIME, STATS_TIME, WRITE_REDIS_TIME
    }

    public static void main(String[] args) {
        try {
            String table = Tables.table(Tables.PH_LONGTEXT_CMT_TBL);
            LOG.info("[TABLE NAME]:" + table);
            new CommentRunnerMR().run(table);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
