package com.datastory.banyan.migrate1;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.HTableInterfacePool;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.utils.BanyanTypeUtil;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.util.LinkedList;

/**
 * com.datastory.banyan.migrate1.WriteBackUserMigrateMR
 *
 * @author lhfcws
 * @since 2016/12/27
 */
public class WriteBackUserMigrateMR {
    public static final byte[] R = "r".getBytes();
    public static final byte[][] fields = {
            "activeness".getBytes(),
//            "meta_group".getBytes(),
            "sources".getBytes()
    };

    public Scan buildScan() {
        Scan scan = HBaseUtils.buildScan();
        scan.addColumn(R, "uid".getBytes());
        for (byte[] field : fields) {
            scan.addColumn(R, field);
        }
        return scan;
    }

    public Job buildJob(Scan scan) throws Exception {
        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        job.setJobName("WriteBackUserMigrateMR");
        TableMapReduceUtil.initTableMapperJob(
                Tables.table(Tables.PH_WBUSER_TBL),
                scan,
                MoveMapper.class,
                NullWritable.class,
                NullWritable.class,
                job
        );
        job.setNumReduceTasks(0);
        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);

        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        job.getConfiguration().set("mapred.reduce.slowstart.completed.maps", "1.0");  // map跑完80% 才跑reducer
        job.getConfiguration().set("mapreduce.job.running.map.limit", "260");  // 留资源给reducer和其他任务，该配置仅在 hadoop2.7+ 生效

        return job;
    }

    public static class MoveMapper extends TableMapper<NullWritable, NullWritable> {
        private RFieldPutter putter;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            putter = new RFieldPutter("dt.rhino.weibo.user.v2");
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            String uid = HBaseUtils.getValue(result, R, "uid".getBytes());
            if (uid == null)
                return;
            String pk = BanyanTypeUtil.sub2PK(uid);

            Put put = new Put(pk.getBytes());
            boolean hasField = false;
            for (byte[] field : fields) {
                String value = HBaseUtils.getValue(result, R, field);
                if (value != null) {
                    put.addColumn(R, field, value.getBytes());
                    hasField = true;
                }
            }

            if (hasField) {
                putter.batchWrite(put);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            putter.flush();
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started.");
        WriteBackUserMigrateMR mr = new WriteBackUserMigrateMR();
        Scan scan = mr.buildScan();
        Job job = mr.buildJob(scan);
        job.waitForCompletion(false);
        System.out.println("[PROGRAM] Program exited.");
    }
}
