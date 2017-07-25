package com.datastory.banyan.migrate1.tools;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.yeezhao.commons.util.AdvFile;
import com.yeezhao.commons.util.ClassUtil;
import com.yeezhao.commons.util.ILineParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * com.datastory.banyan.migrate1.tools.FixCatId
 *
 * @author xiangmin
 * @since 2017/5/7
 */
public class FixCatId {
    static byte[] R = "r".getBytes();
    private static String table = "DS_BANYAN_NEWSFORUM_COMMENT_V1";

    public Scan buildScan() {
        Scan scan = HBaseUtils.buildScan();
        SingleColumnValueFilter filter = new SingleColumnValueFilter("r".getBytes(), "cat_id".getBytes(), CompareFilter.CompareOp.EQUAL, "-1".getBytes());
        scan.setFilter(filter);
        scan.addColumn(R, "site_id".getBytes());
        scan.addColumn(R, "cat_id".getBytes());
        return scan;
    }

    public Job createJob(Scan scan) throws IOException {
        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        Configuration conf = job.getConfiguration();

        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setJobName(this.getClass().getSimpleName() + "-" + table);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJarByClass(this.getClass());
        job.setNumReduceTasks(0);

        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapreduce.job.running.map.limit", "200");

        TableMapReduceUtil.initTableMapperJob(table, scan, FixMapper.class, NullWritable.class, NullWritable.class, job);
        return job;
    }

    public static class FixMapper extends TableMapper<Text, Text> {
        RFieldPutter putter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            System.out.println("hbase table: " + table);
            putter = new RFieldPutter(table);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            String pk = new String(result.getRow());
            String siteId = HBaseUtils.getValue(result, R, "site_id".getBytes());
            Put put = new Put(pk.getBytes());
            String catId = SiteCatMapper.siteId2CatId(siteId);

            if (StringUtils.isNotEmpty(catId)) {
                System.out.println("error site_id: " +
                        (StringUtils.isNotEmpty(siteId) ? "null" : siteId));
            }

            catId = StringUtils.isEmpty(catId) ? "8" : catId;

            put.add(R, "cat_id".getBytes(), catId.getBytes());
            putter.batchWrite(put);
            context.getCounter(ScanFlushESMR.ROW.WRITE).increment(1);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            putter.flush();
        }
    }

    public static class SiteCatMapper implements Serializable {
        private static Map<String, String> mapper = new HashMap<>();

        static {
            init();
            System.out.println(mapper);
        }

        private static void init() {
            try {
                final InputStream in = ClassUtil.getResourceAsInputStream("siteid_to_catid.txt");
                if (in != null)
                    AdvFile.loadFileInLines(in, new ILineParser() {
                        @Override
                        public void parseLine(String s) {
                            String[] items = s.split("\t");
                            mapper.put(items[1], items[2]);
                        }
                    });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public static String siteId2CatId(String siteId) {
            return mapper.get(siteId);
        }
    }

    public static void main(String[] args) throws Exception {
        FixCatId runner = new FixCatId();
        Scan scan = runner.buildScan();
        Job job = runner.createJob(scan);
        job.waitForCompletion(true);
        System.out.println("[PROGRAM] Program exited.");
    }

}
