package com.datastory.banyan.weibo.spark;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.HbaseScanner;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.weibo.analyz.component.ComponentAnalyzer;
import com.xiaoleilu.hutool.util.ClassUtil;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.*;

/**
 * com.datastory.banyan.weibo.spark.ScanContentUidMRAnalyzer
 * 一般建议只指定一个ComponentAnalyzer （用Iterable实现），否则有可能爆内存
 * @author lhfcws
 * @since 2017/1/17
 */
@Deprecated
public class ScanContentUidMRAnalyzer extends HbaseScanner {
    static final byte[] R = "r".getBytes();
    protected List<ComponentAnalyzer> componentAnalyzers = new ArrayList<>();
    protected Set<String> fields = new HashSet<>(Arrays.asList("uid"));

    public List<ComponentAnalyzer> getComponentAnalyzers() {
        return componentAnalyzers;
    }

    public int getComponentAnalyzersSize() {
        return componentAnalyzers.size();
    }

    public void addComponentAnalyzer(ComponentAnalyzer... componentAnalyzer) {
        this.componentAnalyzers.addAll(Arrays.asList(componentAnalyzer));
    }

    public String getAppName() {
        StringBuilder sb = new StringBuilder("ScanContent[");
        for (ComponentAnalyzer componentAnalyzer : componentAnalyzers) {
            sb.append(componentAnalyzer.getClass().getSimpleName()).append(",");
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public Scan buildScan() {
        Scan scan = HBaseUtils.buildScan();
        for (ComponentAnalyzer componentAnalyzer : getComponentAnalyzers()) {
            fields.addAll(componentAnalyzer.getInputFields());
        }

        for (String field : fields) {
            scan.addColumn("r".getBytes(), field.getBytes());
        }

        System.out.println("[SCAN] " + scan);
        this.scan = scan;
        return scan;
    }

    public void run() throws Exception {
        Job job = buildJob();
        job.setJobName(getAppName());
        job.setNumReduceTasks(150);
        job.setReducerClass(AnalyzReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        TableMapReduceUtil.initTableMapperJob(Tables.table(Tables.PH_WBCNT_TBL), scan, ScanMapper.class, Text.class, StrParams.class, job);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        Configuration conf = job.getConfiguration();
        conf.set("banyan.table", table);
        conf.set("banyan.fields", StringUtils.join(fields, ","));

        List<String> analyzers = new ArrayList<>();
        for (ComponentAnalyzer ca : getComponentAnalyzers()) {
            analyzers.add(ca.getClass().getCanonicalName());
        }
        conf.set("banyan.analyzers", StringUtils.join(analyzers, ","));
        conf.set("mapreduce.reduce.memory.mb", "3072");
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.job.running.map.limit", "400");

        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started.");
        ScanContentUidMRAnalyzer runner = new ScanContentUidMRAnalyzer();
        if (args.length > 0) {
            for (String cn : args) {
                if (!cn.contains(".")) {
                    byte[] bs = Base64.decodeBase64(cn.getBytes());
                    cn = new String(bs);
                }
                Class klass = Class.forName(cn);
                ComponentAnalyzer ca = (ComponentAnalyzer) ClassUtil.newInstance(klass);
                runner.addComponentAnalyzer(ca);
            }
        } else
            return;

        System.out.println(runner.getComponentAnalyzers());

        runner.buildScan();
        runner.run();

        System.out.println("[PROGRAM] Program exited.");
    }

    public ScanContentUidMRAnalyzer() {
        super(Tables.table(Tables.PH_WBUSER_TBL));
    }


    // =====================================================
    public static class ScanMapper extends TableMapper<Text, StrParams> {
        protected Set<String> fields = new HashSet<>(Arrays.asList("uid"));

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String[] fs = conf.getStrings("banyan.fields");
            fields.addAll(Arrays.asList(fs));
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            StrParams hbDoc = new StrParams();
            for (String field : fields) {
                hbDoc.put(field, HBaseUtils.getValue(result, R, field.getBytes()));
            }
            String uid = hbDoc.get("uid");
            context.write(new Text(uid), hbDoc);
        }
    }


    public static class AnalyzReducer extends Reducer<Text, StrParams, NullWritable, NullWritable> {
        protected List<ComponentAnalyzer> componentAnalyzers = new ArrayList<>();
        protected RFieldPutter putter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String[] cas = conf.getStrings("banyan.analyzers");
            for (String cn : cas) {
                if (!cn.contains(".")) {
                    byte[] bs = Base64.decodeBase64(cn.getBytes());
                    cn = new String(bs);
                }
                Class klass = null;
                try {
                    klass = Class.forName(cn);
                    ComponentAnalyzer ca = (ComponentAnalyzer) ClassUtil.newInstance(klass);
                    componentAnalyzers.add(ca);
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }

            putter = new RFieldPutter(Tables.table(Tables.PH_WBUSER_TBL));
            putter.setCacheSize(1000);
        }

        @Override
        protected void reduce(Text key, Iterable<StrParams> contents, Context context) throws IOException, InterruptedException {
            String uid = key.toString();
            String pk = BanyanTypeUtil.wbuserPK(uid);

            StrParams ret = new StrParams("pk", pk);

            if (componentAnalyzers.size() == 1) {
                for (ComponentAnalyzer ca : componentAnalyzers) {
                    StrParams res = ca.analyz(uid, contents);
                    ret.putAll(res);
                }
            } else if (componentAnalyzers.size() > 1) {
                List<StrParams> list = new ArrayList<StrParams>();
                // 一个用户的微博数一般有限，而且一般只拿几个需分析字段而已，不会爆内存
                for (StrParams p : contents) {
                    list.add(p);
                }

                for (ComponentAnalyzer ca : componentAnalyzers) {
                    StrParams res = ca.analyz(uid, list);
                    ret.putAll(res);
                }
            }

            if (ret.size() > 1) {
                putter.batchWrite(ret);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            putter.flush();
        }
    }
}
