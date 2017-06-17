package com.datastory.banyan.migrate1.tools;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseReader;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.ListHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.NullComparator;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * com.datastory.banyan.migrate1.tools.FixSrcContent
 *
 * @author lhfcws
 * @since 2017/3/21
 */
public class FixSrcContent {
    static String TABLE = Tables.table(Tables.PH_WBCNT_TBL);
    static byte[] R = "r".getBytes();

    public Scan buildScan() {
        Scan scan = HBaseUtils.buildScan();
        FilterList filterList = new FilterList();
        SingleColumnValueFilter filter1 = new SingleColumnValueFilter("r".getBytes(), "src_content".getBytes(), CompareFilter.CompareOp.EQUAL, new NullComparator());
        SingleColumnValueFilter filter2 = new SingleColumnValueFilter("r".getBytes(), "src_mid".getBytes(), CompareFilter.CompareOp.NOT_EQUAL, new NullComparator());
        filterList.addFilter(filter1);
        filterList.addFilter(filter2);

        scan.setFilter(filterList);
        scan.addColumn(R, "src_content".getBytes());
        scan.addColumn(R, "src_mid".getBytes());
        return scan;
    }

    public Job createJob(Scan scan, String table) throws IOException {
        Configuration conf = RhinoETLConfig.create();
        Job job = Job.getInstance(conf);
        conf = job.getConfiguration();
//        conf.set("hbase.client.keyvalue.maxsize", "" + (50 * 1024 * 1024));
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapred.reduce.slowstart.completed.maps", "1.0");  // map跑完100% 才跑reducer
        conf.set("mapreduce.job.running.map.limit", "200");
        conf.set("mapreduce.reduce.memory.mb", 1024 * 6 + "");
        conf.set("mapreduce.map.memory.mb", "1024");

        job.setJobName(this.getClass().getSimpleName() + "-" + table);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setJarByClass(this.getClass());
        job.setNumReduceTasks(100);
        TableMapReduceUtil.initTableMapperJob(table, scan, FixMapper.class, Text.class, Text.class, job);

        job.setReducerClass(FixReducer.class);
        return job;
    }

    public static class FixMapper extends TableMapper<Text, Text> {
        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            String pk = new String(result.getRow());
            String mid = pk.substring(3, pk.length());
            String srcMid = HBaseUtils.getValue(result, R, "src_mid".getBytes());
            context.getCounter(ScanFlushESMR.ROW.READ).increment(1);
            if (BanyanTypeUtil.valid(srcMid)) {
                String srcContent = HBaseUtils.getValue(result, R, "src_content".getBytes());
                if (!BanyanTypeUtil.valid(srcContent)) {
                    context.getCounter(ScanFlushESMR.ROW.PASS).increment(1);
                    context.write(new Text(srcMid), new Text(mid));
                } else
                    context.getCounter(ScanFlushESMR.ROW.FILTER).increment(1);
            }
        }
    }

    public static class FixReducer extends Reducer<Text, Text, NullWritable, NullWritable> {
        HBaseReader hBaseReader;
        RFieldPutter putter;
        ListHashMap<String, String> listMap = new ListHashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            hBaseReader = new HBaseReader() {
                @Override
                public String getTable() {
                    return TABLE;
                }
            };
            putter = new RFieldPutter(TABLE);
            hBaseReader.setFields(Arrays.asList("content"));
            hBaseReader.setMaxCache(50);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String srcMid = key.toString();
            List<String> list = new LinkedList<>();
            listMap.put(srcMid, list);
            for (Text t : values) {
                String mid = t.toString();
                list.add(mid);
            }

            try {
                List<Params> res = hBaseReader.batchRead(BanyanTypeUtil.wbcontentPK(srcMid));
                flush(res, context);
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException(e);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            try {
                List<Params> res = hBaseReader.flush();
                flush(res, context);
            } catch (Exception e) {
                e.printStackTrace();
                throw new IOException(e);
            }
            putter.flush();
        }

        protected void flush(List<Params> res, Context context) throws IOException {
            if (!BanyanTypeUtil.valid(res))
                return;

            int emptyParams = 0;
            for (Params p : res) {
                if (p == null) {
                    emptyParams ++;
                    continue;
                }

                String pk = p.getString("pk");
                String srcMid = pk.substring(3, pk.length());
                String srcContent = p.getString("content");

                if (BanyanTypeUtil.valid(srcContent) && BanyanTypeUtil.valid(srcMid)) {
                    List<String> list = listMap.get(srcMid);
                    if (BanyanTypeUtil.valid(list)) {
                        for (String mid : list) {
                            Put put = new Put(BanyanTypeUtil.wbcontentPK(mid).getBytes());
                            put.add(R, "src_content".getBytes(), srcContent.getBytes());
                            putter.batchWrite(put);
                            context.getCounter(ScanFlushESMR.ROW.WRITE).increment(1);
                        }
                    } else
                        context.getCounter(ScanFlushESMR.ROW.ERROR).increment(1);
                } else
                    context.getCounter(ScanFlushESMR.ROW.ERROR).increment(1);
            }
            listMap.clear();
            putter.flush();

            if (emptyParams > 0)
                context.getCounter(ScanFlushESMR.ROW.EMPTY).increment(emptyParams);
        }
    }

    public static void main(String[] args) throws Exception {
        FixSrcContent runner = new FixSrcContent();
        Scan scan = runner.buildScan();
        Job job = runner.createJob(scan, TABLE);
        job.waitForCompletion(true);
        System.out.println("[PROGRAM] Program exited.");
    }
}
