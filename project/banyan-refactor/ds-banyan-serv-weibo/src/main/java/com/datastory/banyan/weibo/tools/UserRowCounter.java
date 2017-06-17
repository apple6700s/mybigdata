package com.datastory.banyan.weibo.tools;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.HbaseScanner;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.datastory.banyan.utils.DateUtils;
import org.apache.commons.lang3.StringUtils;
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


/**
 * com.datastory.banyan.weibo.tools.UserRowCounter
 * 不可重用的类，但框架可以重用，作为一些条件性临时一次性count，代替phoenix
 * @author lhfcws
 * @since 2016/12/26
 */
public class UserRowCounter extends HbaseScanner {
    public Scan buildScan() {
        Scan scan = HBaseUtils.buildScan();
        scan.addColumn("r".getBytes(), "wb_cnt".getBytes());
        scan.addColumn("r".getBytes(), "create_date".getBytes());
        return scan;
    }

    public void run() throws Exception {
        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        Configuration conf = job.getConfiguration();
        conf.set("hbase.client.keyvalue.maxsize", "" + (50 * 1024 * 1024));
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapred.reduce.slowstart.completed.maps", "1.0");  // map跑完100% 才跑reducer
        job.setJobName(appName);
        job.setJarByClass(this.getClass());
        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);
        TableMapReduceUtil.initTableMapperJob(
                "DS_BANYAN_WEIBO_USER".getBytes(), buildScan(), CountMapper.class, NullWritable.class, NullWritable.class, job
        );
        job.waitForCompletion(false);
    }

    public UserRowCounter() {
        super("DS_BANYAN_WEIBO_USER");
        setScan(buildScan());
    }

    public static class CountMapper extends TableMapper<NullWritable, NullWritable> {
        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            String wbCnt = HBaseUtils.getValue(result, "r".getBytes(), "wb_cnt".getBytes());
            String createDate = HBaseUtils.getValue(result, "r".getBytes(), "create_date".getBytes());
            if (StringUtils.isEmpty(wbCnt) || "0".equals(wbCnt)) {
                return;
            }
            if (StringUtils.isEmpty(createDate) || !DateUtils.validateDatetime(createDate))
                return;

            context.getCounter(ScanFlushESMR.ROW.PASS).increment(1);
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started.");
        new UserRowCounter().run();
        System.out.println("[PROGRAM] Program exited.");
    }
}
