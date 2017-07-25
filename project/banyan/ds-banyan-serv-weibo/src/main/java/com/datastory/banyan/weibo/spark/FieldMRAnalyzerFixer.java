package com.datastory.banyan.weibo.spark;

import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.HbaseScanner;
import com.datastory.banyan.hbase.RFieldPutter;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.hadoop.hbase.client.Put;
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


/**
 * com.datastory.banyan.weibo.spark.FieldMRAnalyzerFixer
 *
 * @author lhfcws
 * @since 2016/12/29
 */
public class FieldMRAnalyzerFixer extends HbaseScanner {
    static final byte[] R = "r".getBytes();
    static String table;
    static String fields;
    static String dftValues;

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started.");
        table = args[0];
        fields = args[1];
        dftValues = args[2];
        FieldMRAnalyzerFixer fixer = new FieldMRAnalyzerFixer();
        fixer.run();
        System.out.println("[PROGRAM] Program exited.");
    }

    public FieldMRAnalyzerFixer() {
        super(table);
    }

    public void run() throws Exception {
        Scan scan = buildScan();
        scan.setCaching(1000);
        scan.addColumn(R, "update_date".getBytes());
        for (String field : fields.split(",")) {
            scan.addColumn(R, field.getBytes());
        }

        this.setScan(scan);
        Job job = buildJob();

        job.setNumReduceTasks(200);
        job.setReducerClass(FixReducer.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        TableMapReduceUtil.initTableMapperJob(table, scan, FixMapper.class, Text.class, Text.class, job);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        job.getConfiguration().set("banyan.table", table);
        job.getConfiguration().set("banyan.fields", fields);
        job.getConfiguration().set("banyan.dftValues", dftValues);

        job.waitForCompletion(true);
    }

    public static class FixMapper extends TableMapper<Text, Text> {
        private String[] _fields;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            _fields = context.getConfiguration().get("banyan.fields").split(",");
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            Text pk = new Text(new String(result.getRow()));

            for (String f : _fields) {
                String value = HBaseUtils.getValue(result, R, f.getBytes());
                System.out.println(pk + " ==> " + f + " = " + value);
                if (value == null) {
                    context.write(pk, new Text(f));
                }
            }
        }
    }

    public static class FixReducer extends Reducer<Text, Text, NullWritable, NullWritable> {
        private StrParams p = new StrParams();
        private String _table;
        private RFieldPutter putter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String[] _fields = context.getConfiguration().get("banyan.fields").split(",");
            String[] _values = context.getConfiguration().get("banyan.dftValues").split(",");
            for (int i = 0; i < _fields.length; i++) {
                p.put(_fields[i], _values[i]);
            }
            System.out.println("[DFTVALUES] " + p);
            _table = context.getConfiguration().get("banyan.table");
            putter = new RFieldPutter(_table);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String pk = key.toString();
            Put put = new Put(pk.getBytes());
            boolean hasField = false;
            for (Text f : values) {
                String field = f.toString();
                String value = p.get(field);
                if (value != null) {
                    hasField = true;
                    put.addColumn(R, field.getBytes(), value.getBytes());
                }
            }
            if (hasField) {
                int wr = putter.batchWrite(put);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            putter.flush();
        }
    }
}
