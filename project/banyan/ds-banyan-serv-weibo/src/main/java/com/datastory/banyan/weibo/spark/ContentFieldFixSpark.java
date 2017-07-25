package com.datastory.banyan.weibo.spark;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.spark.AbstractSparkRunner;
import com.datastory.banyan.spark.SparkUtil;
import com.datastory.banyan.weibo.analyz.MsgTypeAnalyzer;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Iterator;

/**
 * com.datastory.banyan.weibo.spark.ContentFieldFixSpark
 * It should be move to migrate
 * @author lhfcws
 * @since 2017/1/20
 */
@Deprecated
public class ContentFieldFixSpark extends AbstractSparkRunner {
    public static final byte[] R = "r".getBytes();
    public static String[] inputFields = {
            "src_mid", "rt_mid", "self_content", "content"
    };

    public ContentFieldFixSpark() {
    }

    public Scan buildScan() {
        Scan scan = HBaseUtils.buildScan();
        for (String field : inputFields) {
            scan.addColumn(R, field.getBytes());
        }
        System.out.println(scan.toMap(15));
        return scan;
    }

    public void run(Scan scan) throws IOException {
        Configuration conf = RhinoETLConfig.create();
        conf.set(TableInputFormat.INPUT_TABLE, Tables.table(Tables.PH_WBCNT_TBL));
        conf.set(TableInputFormat.SCAN, HBaseUtils.convertScanToString(scan));
        System.out.println("[SCAN] " + scan.toMap(15));

        StrParams sparkConf = this.customizedSparkConfParams();
        sparkConf.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.put("spark.kryo.registrator", "com.datastory.banyan.spark.BanyanKryoRegister");

//        JavaSparkContext jsc = SparkUtil.createYarnSparkContext(sparkConf);
        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), false, getAppName(), "1000", sparkConf);
        final Accumulator<Integer> accu = jsc.accumulator(0);
        try {
            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                    jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            hBaseRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>>() {
                @Override
                public void call(Iterator<Tuple2<ImmutableBytesWritable, Result>> tuple2Iterator) throws Exception {
                    RFieldPutter putter = new RFieldPutter(Tables.table(Tables.PH_WBCNT_TBL));
                    putter.setCacheSize(4300);

                    while (tuple2Iterator.hasNext()) {
                        accu.add(1);
                        Result result = tuple2Iterator.next()._2();
                        if (result.isEmpty())
                            continue;

                        Params hbDoc = new ResultRDocMapper(result).map();
                        Params outDoc = fix(hbDoc);
                        putter.batchWrite(outDoc);
                    }
                    putter.flush();
                }
            });
            System.out.println("[ACCUMULATOR] " + accu.value());
        } finally {
            jsc.stop();
            jsc.close();
        }
    }

    public Params fix(Params hbDoc) {
        short msgType = MsgTypeAnalyzer.analyz(hbDoc.getString("src_mid"), hbDoc.getString("rt_mid"), hbDoc.getString("self_content"), hbDoc.getString("content"));
        Params ret = new Params("pk", hbDoc.getString("pk"));
        ret.put("msg_type", msgType + "");
        return ret;
    }

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started. PID=" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);
        ContentFieldFixSpark runner = new ContentFieldFixSpark();
        Scan scan = runner.buildScan();
        runner.run(scan);
        System.out.println("[PROGRAM] Program exited.");
    }
}
