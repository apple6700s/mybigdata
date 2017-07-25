package com.datastory.banyan.weibo.analyz.run;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.HTableInterfacePool;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.weibo.analyz.MetaGroupAnalyzer;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.StringUtil;
import com.yeezhao.commons.util.quartz.QuartzExecutor;
import com.yeezhao.commons.util.quartz.QuartzJobUtils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.quartz.SchedulerException;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * com.datastory.banyan.weibo.analyz.run.MetaGroupMRAnalyzer
 *
 * @author lhfcws
 * @since 16/12/4
 */

public class MetaGroupMRAnalyzer implements Serializable {
    private static final String[] FANS_RANGE = {"1000", "500000"}; // 讲道理，这个范围应该是算法组提供接口的。
    public static final String SELF_PREFIX = "_";
    public static final byte[] R = "r".getBytes();
    public static final byte[] UID = "uid".getBytes();
    public static final byte[] TAG_DIST = "tag_dist".getBytes();
    public static final byte[] FANS_CNT = "fans_cnt".getBytes();
    //    public static final byte[] FANS_CNT = "follow_count".getBytes();
    public static final byte[] META_GROUP = "meta_group".getBytes();
    public static final byte[] F = "f".getBytes();
    public static final String table = Tables.table(Tables.PH_WBUSER_TBL);
//    public static final String table = "dt.rhino.weibo.user.v2";

    public void run() throws IOException, ClassNotFoundException, InterruptedException {
        Scan scan = buildScan();
        Job job = buildJob(scan);
        job.waitForCompletion(true);
    }

    public Scan buildScan() {
        Scan scan = HBaseUtils.buildScan();
        scan.addFamily(F);
        scan.addColumn(R, UID);
        scan.addColumn(R, TAG_DIST);

        SingleColumnValueFilter filter1 = new SingleColumnValueFilter(R, FANS_CNT, CompareFilter.CompareOp.GREATER_OR_EQUAL, FANS_RANGE[0].getBytes());
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter(R, FANS_CNT, CompareFilter.CompareOp.LESS_OR_EQUAL, FANS_RANGE[1].getBytes());
        FilterList filterList = new FilterList();
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);
        scan.setFilter(filterList);

        return scan;
    }

    public Job buildJob(Scan scan) throws IOException {

        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(40);
        job.setReducerClass(MetaGroupAnalyzReducer.class);
        job.setJarByClass(this.getClass());
        job.setJobName("MetaGroupMRAnalyzer-" + DateUtils.getCurrentHourStr());

        TableMapReduceUtil.initTableMapperJob(table, scan, MetaGroupScanMapper.class, Text.class, Text.class, job);

        job.getConfiguration().set("banyan.table", table);
        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        job.getConfiguration().set("mapred.reduce.slowstart.completed.maps", "1.0");  // map跑完80% 才跑reducer
        job.getConfiguration().set("mapreduce.job.running.map.limit", "260");  // 留资源给reducer和其他任务，该配置仅在 hadoop2.7+ 生效
        job.getConfiguration().set("mapreduce.reduce.memory.mb", "" + 2048 * 4);
        return job;
    }

    /**
     * MR Mapper
     */
    public static class MetaGroupScanMapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            String tag_dist = HBaseUtils.getValue(result, R, TAG_DIST);
            if (StringUtil.isNullOrEmpty(tag_dist))
                return;

            String uid = HBaseUtils.getValue(result, R, UID);
            Text uidText = new Text(uid);
            Text tdText = new Text(tag_dist);

            Map<String, String> fansMap = HBaseUtils.getValues(result, F);
            context.write(uidText, new Text(SELF_PREFIX + tag_dist));
            if (fansMap != null && !fansMap.isEmpty())
                for (String fans : fansMap.keySet()) {
                    context.write(new Text(fans), tdText);
                }
        }
    }

    /**
     * MR Reducer
     */
    public static class MetaGroupAnalyzReducer extends Reducer<Text, Text, NullWritable, NullWritable> {
        private MetaGroupAnalyzer metaGroupAnalyzer = null;
        private RFieldPutter putter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String table = conf.get("banyan.table");
            putter = new RFieldPutter(table);

            metaGroupAnalyzer = new MetaGroupAnalyzer();
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                String uid = key.toString();
                String pk = BanyanTypeUtil.wbuserPK(uid);
                String metaGroup = metaGroupAnalyzer.analyz(values);
                Put put = new Put(pk.getBytes());
                put.addColumn(R, META_GROUP, metaGroup.getBytes());
                putter.batchWrite(put);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            putter.flush();
        }
    }

    /**
     * Cli entry.
     */
    public static class Cli implements CliRunner, QuartzExecutor {
        public static final String CRON = "0 0 22 1/10 * ?";

        @Override
        public Options initOptions() {
            Options options = new Options();
            options.addOption("q", false, "quartz");
            return options;
        }

        @Override
        public boolean validateOptions(CommandLine commandLine) {
            return true;
        }

        @Override
        public void start(CommandLine commandLine) {
            if (commandLine.hasOption("q")) {
                try {
                    QuartzJobUtils.createQuartzJob(CRON, "MetaGroupMRAnalyzer", new Cli());
                } catch (SchedulerException e) {
                    e.printStackTrace();
                }
            } else {
                execute();
            }

        }

        @Override
        public void execute() {
            MetaGroupMRAnalyzer metaGroupMRAnalyzer = new MetaGroupMRAnalyzer();
            try {
                metaGroupMRAnalyzer.run();
            } catch (IOException | ClassNotFoundException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * MAIN FUNCTION
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        AdvCli.initRunner(args, "MetaGroupMRAnalyzer", new Cli());

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
