package com.datastory.banyan.spark;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;
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

import java.io.IOException;

/**
 * com.datastory.banyan.spark.ScanMR
 *
 * @author lhfcws
 * @since 2017/7/11
 */
public class ScanMR {

    public int getReducerNum() {
        return 40;
    }

    public Scan buildScan(Filter filter) {
        Scan scan = HBaseUtils.buildScan();
        if (filter != null)
            scan.setFilter(filter);
        return scan;
    }

    public Scan buildAllScan() {
        return buildScan(null);
    }

    public Scan buildScan() {
        return buildAllScan();
    }

    public Job buildJob(String table, Scan scan, Class<? extends TableMapper> mapperClass) throws IOException {
        System.out.println("[SCAN] " + table + ";  " + scan);
        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        Configuration conf = job.getConfiguration();

        job.setJarByClass(this.getClass());
        job.setInputFormatClass(TableInputFormat.class);
        job.setJobName(this.getClass().getSimpleName() + "-" + table);
        TableMapReduceUtil.initTableMapperJob(table, scan, mapperClass, Text.class, Params.class, job);

        conf.set("hbase.client.keyvalue.maxsize", "" + (50 * 1024 * 1024));
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapred.reduce.slowstart.completed.maps", "1.0");  // map跑完100% 才跑reducer
        conf.set("mapreduce.job.running.map.limit", "200");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.map.memory.mb", "2048");

//        conf.set("mapred.job.reuse.jvm.num.tasks", "1");
        return job;
    }

    public void initReducer(Job job, Class<? extends Reducer> reducerClass) {
        if (reducerClass != null)
            job.setReducerClass(reducerClass);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        if (reducerClass != null)
            job.setNumReduceTasks(getReducerNum());
        else
            job.setNumReduceTasks(0);
    }

    public static abstract class Mapper_ extends TableMapper<Text, Params> {
        protected Params transform(Result result) {
            Params p = new ResultRDocMapper(result).map();
            return p;
        }

        protected abstract void _process(Params p);

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            if (result.isEmpty())
                return;

            Params p = transform(result);
            if (BanyanTypeUtil.valid(p)) {
                _process(p);
            }
        }
    }
}
