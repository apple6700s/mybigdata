package com.datastory.banyan.weibo.spark;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.spark.AbstractSparkRunner;
import com.datastory.banyan.spark.SparkUtil;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.weibo.analyz.user_component.UserAnalyzer;
import com.xiaoleilu.hutool.util.ClassUtil;
import com.yeezhao.commons.util.Entity.StrParams;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * com.datastory.banyan.weibo.spark.ScanUserSparkAnalyzer
 * 一般建议只指定一个UserAnalyzer （用Iterable实现），否则有可能爆内存
 * @author lhfcws
 * @since 2017/1/5
 */
public class ScanUserSparkAnalyzer extends AbstractSparkRunner {
    static Logger LOG = Logger.getLogger(ScanContentUidSparkAnalyzer.class);
    static final byte[] R = "r".getBytes();
    protected Set<String> fields = new HashSet<>(Arrays.asList("uid"));
    protected List<UserAnalyzer> analyzers = new LinkedList<>();

    public List<UserAnalyzer> getAnalyzers() {
        return analyzers;
    }

    public void addAnalyzer(UserAnalyzer userAnalyzer) {
        getAnalyzers().add(userAnalyzer);
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.cores.max", "210");
        sparkConf.put("spark.executor.memory", "4000m");
//        sparkConf.put("spark.executor.cores", "2");
//        sparkConf.put("spark.executor.instances", "100");
        return sparkConf;
    }

    public String getAppName() {
        StringBuilder sb = new StringBuilder("ScanUser[");
        for (UserAnalyzer analyzer : getAnalyzers()) {
            sb.append(analyzer.getClass().getSimpleName()).append(",");
        }
        sb.append("]");
        return sb.toString();
    }

    public Scan buildScanner() {
        Scan scan = HBaseUtils.buildScan();
        for (UserAnalyzer analyzer : getAnalyzers()) {
            System.out.println(analyzer + " : " + analyzer.getInputFields());
            fields.addAll(analyzer.getInputFields());
        }

        for (String field : fields) {
            scan.addColumn("r".getBytes(), field.getBytes());
        }

        return scan;
    }

    public void run(Scan scan) throws IOException {
        if (getAnalyzers().isEmpty())
            return;

        Configuration conf = RhinoETLConfig.getInstance();
        conf.set(TableInputFormat.INPUT_TABLE, Tables.table(Tables.PH_WBUSER_TBL));
        conf.set(TableInputFormat.SCAN, HBaseUtils.convertScanToString(scan));
        System.out.println("[SCAN] " + scan.toMap(15));
        System.out.println(getAnalyzers());

        StrParams sparkConf = this.customizedSparkConfParams();
        sparkConf.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.put("spark.kryo.registrator", "com.datastory.banyan.spark.BanyanKryoRegister");

//        JavaSparkContext jsc = SparkUtil.createYarnSparkContext(sparkConf);
        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), false, getAppName(), "700", sparkConf);
        try {
            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                    jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

            hBaseRDD
                    // local
                    .flatMap(new FlatMapFunction<Tuple2<ImmutableBytesWritable,Result>, StrParams>() {
                        @Override
                        public Iterable<StrParams> call(Tuple2<ImmutableBytesWritable, Result> immutableBytesWritableResultTuple2) throws Exception {
                            Result result = immutableBytesWritableResultTuple2._2();
                            if (result.isEmpty())
                                return new ArrayList<>();

                            StrParams hbDoc = new StrParams();
                            for (String field : fields) {
                                hbDoc.put(field, HBaseUtils.getValue(result, R, field.getBytes()));
                            }

                            return Arrays.asList(hbDoc);
                        }
                    })
                    .repartition(700)
                    .persist(StorageLevel.MEMORY_AND_DISK_SER())
                    // local
                    .flatMap(new FlatMapFunction<StrParams, StrParams>() {
                        @Override
                        public Iterable<StrParams> call(StrParams user) throws Exception {
                            String pk = BanyanTypeUtil.wbuserPK(user.get("uid"));
                            StrParams ret = new StrParams("pk", pk);

                            for (UserAnalyzer analyzer : getAnalyzers()) {
                                StrParams p = analyzer.analyz(user);
                                ret.putAll(p);
                            }
                            return Arrays.asList(ret);
                        }
                    })
                    // local
                    .foreachPartition(new VoidFunction<Iterator<StrParams>>() {
                        @Override
                        public void call(Iterator<StrParams> iter) throws Exception {
                            RFieldPutter putter = new RFieldPutter(Tables.ANALYZ_USER_TBL);
                            putter.setCacheSize(3000);
                            while (iter.hasNext()) {
                                StrParams p = iter.next();
                                int wr = putter.batchWrite(p);
                            }
                            putter.flush();
                        }
                    });
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            jsc.stop();
            jsc.close();
        }
    }


    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started.");
        ScanUserSparkAnalyzer runner = new ScanUserSparkAnalyzer();
        if (args.length > 0) {
            for (String cn : args) {
                if (!cn.contains(".")) {
                    byte[] bs = Base64.decodeBase64(cn.getBytes());
                    cn = new String(bs);
                }
                Class klass = Class.forName(cn);
                UserAnalyzer ca = (UserAnalyzer) ClassUtil.newInstance(klass);
                runner.addAnalyzer(ca);
            }
        }

        System.out.println(runner.getAnalyzers());

        Scan scan = runner.buildScanner();
        runner.run(scan);

        System.out.println("[PROGRAM] Program exited.");
    }

}
