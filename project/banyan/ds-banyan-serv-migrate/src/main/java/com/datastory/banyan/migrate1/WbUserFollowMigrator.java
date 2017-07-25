package com.datastory.banyan.migrate1;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.HTableInterfacePool;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.datastory.banyan.spark.SparkUtil;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
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
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * com.datastory.banyan.migrate1.WbUserFollowMigrator
 *
 * @author lhfcws
 * @since 16/12/8
 */

public class WbUserFollowMigrator implements Serializable {
    static final byte[] FOLLOW = "follow".getBytes();
    static final byte[] F = "f".getBytes();

    protected int cores = 600;

    public void run(Filter filter) throws Exception {
        Scan scan = buildAllScan();
        if (filter != null)
            scan.setFilter(filter);
//        buildSpark("dt.rhino.weibo.user.v2", scan);
        Job job = buildJob("dt.rhino.weibo.user.v2", scan);
        job.waitForCompletion(true);
    }

    public Scan buildAllScan() {
        Scan scan = HBaseUtils.buildScan();
        scan.setCaching(100);
//        scan.addFamily(FOLLOW);
        return scan;
    }

    public void buildSpark(String table, Scan scan) throws IOException {
        Configuration conf = new Configuration(RhinoETLConfig.getInstance());
        conf.set(TableInputFormat.INPUT_TABLE, table);
        conf.set(TableInputFormat.SCAN, HBaseUtils.convertScanToString(scan));
        conf.set("hbase.rpc.timeout", "1500000");
        conf.set("hbase.client.scanner.timeout.period", "1500000");

        StrParams sparkConf = new StrParams();
        sparkConf.put("spark.executor.memory", "1500m");
        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), false, this.getClass().getSimpleName(), cores + "", sparkConf);
        final Accumulator<Integer> accumulator = jsc.accumulator(0, "users");
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
        hBaseRDD
                .foreachPartition(new VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>>() {
                    @Override
                    public void call(Iterator<Tuple2<ImmutableBytesWritable, Result>> iterator) throws Exception {
                        Migrator migrator = Migrator.getInstance();
                        while (iterator.hasNext()) {
                            Result result = iterator.next()._2();
                            accumulator.add(1);
                            if (result.isEmpty())
                                continue;

                            migrator.process(result);
                        }
                        migrator.cleanup();
                    }
                });

    }

    public Job buildJob(String table, Scan scan) throws IOException {
        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        Configuration conf = job.getConfiguration();

        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setJobName(this.getClass().getSimpleName() + "-" + table);
        TableMapReduceUtil.initTableMapperJob(table, scan, ScanMapper.class, NullWritable.class, NullWritable.class, job);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJarByClass(this.getClass());

        job.setNumReduceTasks(0);
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapred.reduce.slowstart.completed.maps", "1.0");  // map跑完80% 才跑reducer
        conf.set("mapreduce.job.running.map.limit", "400");
        conf.set("hbase.rpc.timeout", "1500000");
        conf.set("hbase.client.scanner.timeout.period", "1500000");
//        conf.set("mapred.job.reuse.jvm.num.tasks", "1");
        return job;
    }

    public static class Migrator implements Serializable {
        private static volatile Migrator _singleton = null;

        public static Migrator getInstance() throws IOException {
            if (_singleton == null)
                synchronized (Migrator.class) {
                    if (_singleton == null) {
                        _singleton = new Migrator();
                    }
                }
            return _singleton;
        }

        public static Migrator getInstance(int num) throws IOException {
            if (_singleton == null)
                synchronized (Migrator.class) {
                    if (_singleton == null) {
                        _singleton = new Migrator(num);
                    }
                }
            return _singleton;
        }

        protected HTableInterface hti = null;
        protected List<Put> cache = new LinkedList<>();
        protected int cacheSize = 1000;

        public Migrator() throws IOException {
            hti = HTableInterfacePool.get(Tables.table(Tables.PH_WBUSER_TBL));
        }

        public Migrator(int cacheSize) throws IOException {
            this();
            this.cacheSize = cacheSize;
        }

        public void process(Result result) throws IOException {
            if (result == null || result.isEmpty())
                return;
            String pk = new String(result.getRow());
            if (pk.length() < 4)
                return;

            String uid = pk.substring(2);

            String newPK = BanyanTypeUtil.wbuserPK(uid);
            Put put = new Put(newPK.getBytes());
            Map<byte[], byte[]> bm = result.getFamilyMap(FOLLOW);
            if (bm != null && !bm.isEmpty()) {
                for (Map.Entry<byte[], byte[]> e : bm.entrySet()) {
                    put.addColumn(F, e.getKey(), e.getValue());
                }
                cache.add(put);
            }

            if (cache.size() > cacheSize) {
                flush();
            }
        }

        public synchronized void flush() throws IOException {
            if (cache.size() > 0) {
                hti.put(cache);
                cache.clear();
                hti.flushCommits();
            }
        }

        public void cleanup() throws IOException {
            if (hti != null) {
                flush();
                HTableInterfacePool.close(hti);
            }
        }
    }

    /**
     * Mapper
     */
    public static class ScanMapper extends TableMapper<NullWritable, NullWritable> implements Serializable {
        Migrator migrator = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            migrator = new Migrator();
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException, InterruptedException {
            context.getCounter(ScanFlushESMR.ROW.READ).increment(1);
            Map<byte[], byte[]> bm = result.getFamilyMap(FOLLOW);
            if (bm == null || bm.isEmpty())
                context.getCounter(ScanFlushESMR.ROW.FILTER).increment(1);
            else
                context.getCounter(ScanFlushESMR.ROW.PASS).increment(1);
            migrator.process(result);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (migrator != null)
                migrator.cleanup();
        }
    }

    public static void main(String[] args) throws Exception {
        long mainStartTime = System.currentTimeMillis();
        System.out.println("System started. " + new Date());

        WbUserFollowMigrator fmig = new WbUserFollowMigrator();

        FilterList filterList = new FilterList();
        if (args.length >= 1) {
            String startUpdateDate = args[0];
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    "r".getBytes(), "update_date".getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, startUpdateDate.getBytes()
            );
            filterList.addFilter(filter);
        }

        if (args.length >= 2) {
            String endUpdateDate = args[1];
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    "r".getBytes(), "update_date".getBytes(), CompareFilter.CompareOp.LESS_OR_EQUAL, endUpdateDate.getBytes()
            );
            filterList.addFilter(filter);
        }
        if (filterList.getFilters().isEmpty()) {
            System.out.println("Run follow migrate. " + new Date());
            fmig.run(null);
        } else {
            System.out.println("Run follow migrate. " + new Date());
            fmig.run(filterList);
        }

        long mainEndTime = System.currentTimeMillis();
        System.out.println("Program exited. " + new Date() + " , cost time(ms): " + (mainEndTime - mainStartTime));
    }
}
