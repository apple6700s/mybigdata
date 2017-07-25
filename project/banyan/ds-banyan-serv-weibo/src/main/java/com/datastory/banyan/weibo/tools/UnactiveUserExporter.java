package com.datastory.banyan.weibo.tools;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.spark.MREnum;
import com.datastory.banyan.spark.ScanMR;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.weibo.abel.Tables;
import com.datatub.buffalo.client.job_params.JobParamsClient;
import com.datatub.buffalo.client.job_params.JobParamsClientFactory;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Calendar;

/**
 * com.datastory.banyan.weibo.tools.UnactiveUserExporter
 * 3个月一次
 *
 * @author lhfcws
 * @since 2017/7/11
 */
public class UnactiveUserExporter extends ScanMR {
    public static final String OUTPUT = "/tmp/banyan/update/UnactiveUserExporter";

    @Override
    public Scan buildScan() {
        Scan scan = super.buildScan();

        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, -90);
        String deadline = DateUtils.getTimeStr(calendar.getTime());

        FilterList filterList = new FilterList();
        filterList.addFilter(
                new SingleColumnValueFilter("r".getBytes(), "user_type".getBytes(),
                        CompareFilter.CompareOp.NOT_EQUAL, "2".getBytes()));
        filterList.addFilter(
                new SingleColumnValueFilter("r".getBytes(), "update_date".getBytes(),
                        CompareFilter.CompareOp.LESS, deadline.getBytes())
        );

        scan.setFilter(filterList);
        scan.addColumn("r".getBytes(), "update_date".getBytes());
        scan.addColumn("r".getBytes(), "user_type".getBytes());

        return scan;
    }

    @Override
    public Job buildJob(String table, Scan scan, Class<? extends TableMapper> mapperClass) throws IOException {
        System.out.println("[SCAN] " + table + ";  " + scan);
        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        Configuration conf = job.getConfiguration();

        job.setJarByClass(this.getClass());
        job.setInputFormatClass(TableInputFormat.class);
        job.setJobName(this.getClass().getSimpleName() + "-" + table);
        TableMapReduceUtil.initTableMapperJob(table, scan, mapperClass, NullWritable.class, Text.class, job);

        job.setNumReduceTasks(0);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileSystem fs = FileSystem.get(conf);

        Path outputRoot = new Path(OUTPUT);
        fs.mkdirs(outputRoot);

        String outputDir = outputRoot + "/" + DateUtils.getCurrentTimeStr();
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        conf.set("datastory.output", outputDir);
        conf.set("hbase.client.keyvalue.maxsize", "" + (50 * 1024 * 1024));
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapred.reduce.slowstart.completed.maps", "1.0");  // map跑完100% 才跑reducer
        conf.set("mapreduce.job.running.map.limit", "200");
        conf.set("mapreduce.reduce.memory.mb", "4096");
        conf.set("mapreduce.map.memory.mb", "2048");

//        conf.set("mapred.job.reuse.jvm.num.tasks", "1");
        return job;
    }

    public static class Mapper_ extends TableMapper<NullWritable, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result result, Mapper.Context context) throws IOException, InterruptedException {
            if (result.isEmpty())
                return;
            context.getCounter(MREnum.ROWS).increment(1);

            String rowKey = new String(result.getRow());
            String uid = rowKey.substring(3);
            context.write(NullWritable.get(), new Text(uid));
        }
    }

    public static void start() throws InterruptedException, IOException, ClassNotFoundException {
        UnactiveUserExporter runner = new UnactiveUserExporter();
        Scan scan = runner.buildScan();
        Job job = runner.buildJob(Tables.table(Tables.PH_WBUSER_TBL), scan, Mapper_.class);
        boolean success = job.waitForCompletion(true);
        if (success) {
            String outputDir = job.getConfiguration().get("datastory.output");
//            FileSystemHelper fsUtil = FileSystemHelper.getInstance(job.getConfiguration());
            String resFile = outputDir;
//            String resFile = outputDir + ".txt";
//            fsUtil.mergeDirsToFile(resFile, outputDir);
//            fsUtil.deleteFile(outputDir);

            JobParamsClient jobParamsClient = null;
            try {
                jobParamsClient = JobParamsClientFactory.getClient();
                Params jobParams = jobParamsClient.getJobParams();
                jobParams.put("UnactiveUserExporter.output", resFile);
                jobParamsClient.setJobParams(jobParams);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        start();
        System.out.println("[PROGRAM] Program exited.");
    }
}
