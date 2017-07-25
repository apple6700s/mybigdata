package com.datastory.banyan.hbase;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.spark.SparkUtil;
import com.datastory.commons.spark.client.SparkYarnClusterSubmit;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.io.Serializable;

/**
 * com.datastory.banyan.hbase.HbaseScanner
 *
 * @author lhfcws
 * @since 16/11/22
 */

public class HbaseScanner implements Serializable {
    protected String appName = null;
    protected String table;
    protected Scan scan;
    protected Function<Result, Void> scanFunc = null;
    protected SparkHBaseScanner sparkHBaseScanner = null;

    public HbaseScanner(String table) {
        this.table = table;
    }

    public HbaseScanner(String table, Scan scan) {
        this.table = table;
        this.scan = scan;
    }

    public HbaseScanner(String table, Scan scan, Function<Result, Void> scanFunc) {
        this.table = table;
        this.scan = scan;
        this.scanFunc = scanFunc;
    }

    public Scan buildScan() {
        Scan scan = HBaseUtils.buildScan();
        return scan;
    }

    public Job buildJob() throws Exception {
        System.out.println("[SCAN] " + scan);
        Job job = Job.getInstance(RhinoETLConfig.getInstance());
        Configuration conf = job.getConfiguration();

        job.setInputFormatClass(TableInputFormat.class);
        job.setJobName(this.getClass().getSimpleName() + "-" + table);
        job.setJarByClass(this.getClass());

        conf.set("hbase.client.keyvalue.maxsize", "" + (50 * 1024 * 1024));
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapred.reduce.slowstart.completed.maps", "1.0");  // map跑完100% 才跑reducer
        conf.set("mapreduce.job.running.map.limit", "200");
        conf.set("mapreduce.reduce.memory.mb", "3072");
        conf.set("mapreduce.map.memory.mb", "2048");

        return job;
    }

    public SparkHBaseScanner getSparkHBaseScanner() {
        return sparkHBaseScanner;
    }

    public void setScan(Scan scan) {
        this.scan = scan;
    }

    public void setScanFunc(Function<Result, Void> scanFunc) {
        this.scanFunc = scanFunc;
    }

    public int localScan() throws IOException {
        HTableInterface hTableInterface = HTableInterfacePool.get(table);
        int cnt = 0;
        try {
            ResultScanner scanner = hTableInterface.getScanner(scan);
            int scanBatch = 100;
            while (true) {
                Result[] results = scanner.next(scanBatch);
                if (results == null || results.length == 0)
                    break;
                for (Result result : results) {
                    cnt++;
                    if (scanFunc != null && !result.isEmpty()) {
                        scanFunc.apply(result);
                    }
                }
            }
        } finally {
            hTableInterface.close();
        }
        return cnt;
    }

    public JavaPairRDD<ImmutableBytesWritable, Result> sparkScan(Class<? extends Serializable> klass, int cores, boolean onYarn) throws IOException {
        if (this.appName == null)
            this.appName = klass.getSimpleName() + ": " + scan;
        sparkHBaseScanner = new SparkHBaseScanner(appName, table, HBaseUtils.convertScanToString(scan)).onYarn(onYarn);
        return sparkHBaseScanner.sparkScan(klass, cores);
    }

    public JavaPairRDD<ImmutableBytesWritable, Result> sparkScan(Class<? extends Serializable> klass, int cores, boolean onYarn, StrParams sparkConf) throws IOException {
        if (this.appName == null)
            this.appName = klass.getSimpleName() + ": " + scan;
        sparkHBaseScanner = new SparkHBaseScanner(appName, table, HBaseUtils.convertScanToString(scan)).onYarn(onYarn);
        return sparkHBaseScanner.sparkScan(klass, cores, sparkConf);
    }

    public static class SparkHBaseScanner implements Serializable {
        protected String appName = this.getClass().getSimpleName();
        protected String table;
        protected String scanStr;
        protected boolean onYarn = false;
        protected JavaSparkContext jsc;

        public SparkHBaseScanner() {
        }

        public SparkHBaseScanner(String appName, String table, String scanStr) {
            this.appName = appName;
            this.table = table;
            this.scanStr = scanStr;
            if (this.scanStr == null)
                try {
                    this.scanStr = HBaseUtils.convertScanToString(HBaseUtils.buildScan());
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }

        public SparkHBaseScanner setTable(String table) {
            this.table = table;
            return this;
        }

        public SparkHBaseScanner setAppName(String appName) {
            this.appName = appName;
            return this;
        }

        public SparkHBaseScanner setScan(Scan scan) {
            try {
                this.scanStr = HBaseUtils.convertScanToString(scan);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return this;
        }

        public SparkHBaseScanner onYarn(boolean onYarn) {
            this.onYarn = onYarn;
            return this;
        }

        public JavaSparkContext getJsc() {
            return jsc;
        }

        public JavaPairRDD<ImmutableBytesWritable, Result> sparkScan(Class<? extends Serializable> klass, int cores) {
            Configuration conf = new Configuration(RhinoETLConfig.getInstance());
            conf.set(TableInputFormat.SCAN, scanStr);
            conf.set(TableInputFormat.INPUT_TABLE, table);
            System.out.println(table + " => " + scanStr);

            if (onYarn)
                jsc = new JavaSparkContext(SparkYarnClusterSubmit.getLocalOrNewSparkConf());
            else
                jsc = SparkUtil.createSparkContext(klass, false, this.appName, cores + "");

            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                    jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            return hBaseRDD;
        }

        public JavaPairRDD<ImmutableBytesWritable, Result> sparkScan(Class<? extends Serializable> klass, int cores, StrParams sparkConf) {
            Configuration conf = new Configuration(RhinoETLConfig.getInstance());
            conf.set(TableInputFormat.SCAN, scanStr);
            conf.set(TableInputFormat.INPUT_TABLE, table);
            System.out.println(table + " => " + scanStr);

            if (onYarn)
                jsc = new JavaSparkContext(SparkYarnClusterSubmit.getLocalOrNewSparkConf());
            else
                jsc = SparkUtil.createSparkContext(klass, false, this.appName, cores + "", sparkConf);

            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                    jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            return hBaseRDD;
        }
    }
}
