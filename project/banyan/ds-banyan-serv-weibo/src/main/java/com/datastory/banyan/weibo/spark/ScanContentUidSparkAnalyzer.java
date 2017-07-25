package com.datastory.banyan.weibo.spark;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.spark.AbstractSparkRunner;
import com.datastory.banyan.spark.SparkUtil;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.weibo.analyz.component.ComponentAnalyzer;
import com.xiaoleilu.hutool.util.ClassUtil;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.serialize.ProtostuffSerializer;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * com.datastory.banyan.weibo.spark.ScanContentUidSparkAnalyzer
 * <p>
 * scan 微博全表提取uid做分析，通过自己组合 ComponentAnalyzer 指定分析字段
 * 一般建议只指定一个ComponentAnalyzer （用Iterable实现），否则有可能爆内存
 *
 * @author lhfcws
 * @since 2016/12/23
 */
public class ScanContentUidSparkAnalyzer extends AbstractSparkRunner {
    static Logger LOG = Logger.getLogger(ScanContentUidSparkAnalyzer.class);
    static final byte[] R = "r".getBytes();
    protected Set<String> fields = new HashSet<>(Arrays.asList("uid"));
    protected List<ComponentAnalyzer> componentAnalyzers = new ArrayList<>();
//    protected boolean spoutToESKafka = false;

    public static void main(String[] args) throws Exception {
        System.out.println("[PROGRAM] Program started.");
        ScanContentUidSparkAnalyzer runner = new ScanContentUidSparkAnalyzer();
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
        }

        System.out.println(runner.getComponentAnalyzers());

        Scan scan = runner.buildScanner();
        runner.run(scan);

        System.out.println("[PROGRAM] Program exited.");
    }

    @Override
    public StrParams customizedSparkConfParams() {
        StrParams sparkConf = super.customizedSparkConfParams();
        sparkConf.put("spark.cores.max", "200");
        sparkConf.put("spark.executor.memory", "7000m");
//        sparkConf.put("spark.executor.cores", "2");
//        sparkConf.put("spark.executor.instances", "200");
        return sparkConf;
    }

    public List<ComponentAnalyzer> getComponentAnalyzers() {
        return componentAnalyzers;
    }

    public int getComponentAnalyzersSize() {
        return componentAnalyzers.size();
    }

    public String getAppName() {
        StringBuilder sb = new StringBuilder("ScanContent[");
        for (ComponentAnalyzer componentAnalyzer : componentAnalyzers) {
            sb.append(componentAnalyzer.getClass().getSimpleName()).append(",");
        }
        sb.append("]");
        return sb.toString();
    }

    public Scan buildScanner() {
        Scan scan = HBaseUtils.buildScan();
        for (ComponentAnalyzer componentAnalyzer : getComponentAnalyzers()) {
            fields.addAll(componentAnalyzer.getInputFields());
        }

        for (String field : fields) {
            scan.addColumn("r".getBytes(), field.getBytes());
        }

        System.out.println("[SCAN] " + scan);
        return scan;
    }

    public void addComponentAnalyzer(ComponentAnalyzer... componentAnalyzer) {
        this.componentAnalyzers.addAll(Arrays.asList(componentAnalyzer));
    }


    public void run(Scan scan) throws IOException {
        if (componentAnalyzers.isEmpty())
            return;

        Configuration conf = RhinoETLConfig.getInstance();
        conf.set(TableInputFormat.INPUT_TABLE, Tables.table(Tables.PH_WBCNT_TBL));
        conf.set(TableInputFormat.SCAN, HBaseUtils.convertScanToString(scan));
        System.out.println("[SCAN] " + scan.toMap(15));

        StrParams sparkConf = this.customizedSparkConfParams();
        sparkConf.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.put("spark.kryo.registrator", "com.datastory.banyan.spark.BanyanKryoRegister");

//        JavaSparkContext jsc = SparkUtil.createYarnSparkContext(sparkConf);
        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), false, getAppName(), "500", sparkConf);
        final Accumulator<Integer> accu = jsc.accumulator(0);
        try {
            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                    jsc.newAPIHadoopRDD(conf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);

//            JavaPairRDD<String, Iterable<byte[]>> pairRDD =
                    hBaseRDD
                            // local
                            .flatMapToPair(new PairFlatMapFunction<Tuple2<ImmutableBytesWritable, Result>, String, byte[]>() {
                                @Override
                                public Iterable<Tuple2<String, byte[]>> call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
                                    Result result = v1._2();
                                    if (result.isEmpty())
                                        return new ArrayList<>();

                                    StrParams hbDoc = new StrParams();
                                    for (String field : fields) {
                                        hbDoc.put(field, HBaseUtils.getValue(result, R, field.getBytes()));
                                    }
                                    String uid = hbDoc.get("uid");

                                    StrParams.StrParamsPojo pojo = new StrParams.StrParamsPojo(hbDoc);
                                    byte[] bs = ProtostuffSerializer.serializeObject(pojo);
                                    return Arrays.asList(new Tuple2<>(uid, bs));
                                }
                            })
                            // shuffle and repartition
                            .groupByKey(1200)
//                            .persist(StorageLevel.DISK_ONLY());
//            pairRDD.count();
//            JavaRDD<StrParams> rdd =
//            pairRDD
                    // local
                    .flatMap(new FlatMapFunction<Tuple2<String, Iterable<byte[]>>, StrParams>() {
                        @Override
                        public Iterable<StrParams> call(Tuple2<String, Iterable<byte[]>> v1) throws Exception {
                            Iterable<byte[]> contents = v1._2();
                            accu.add(1);
                            String uid = v1._1();
                            String pk = BanyanTypeUtil.wbuserPK(uid);

                            StrParams ret = new StrParams("pk", pk);

                            if (getComponentAnalyzersSize() == 1) {
                                for (ComponentAnalyzer ca : getComponentAnalyzers()) {
                                    StrParams res = ca.analyz(uid, contents);
                                    ret.putAll(res);
                                }
                            }
//                            else if (getComponentAnalyzersSize() > 1) {
//                                List<StrParams> list = new ArrayList<StrParams>();
//                                // 一个用户的微博数一般有限，而且一般只拿几个需分析字段而已，不会爆内存
//                                for (byte[] p : contents) {
//                                    list.add(p);
//                                }
//
//                                for (ComponentAnalyzer ca : getComponentAnalyzers()) {
//                                    StrParams res = ca.analyz(uid, list);
//                                    ret.putAll(res);
//                                }
//                            }

                            if (ret.size() > 1)
                                return Arrays.asList(ret);
                            else
                                return new ArrayList<StrParams>();
                        }
                    })
//                    .persist(StorageLevel.MEMORY_AND_DISK());
//            long l = rdd.count();
//            rdd
                    // local
                    .foreachPartition(new VoidFunction<Iterator<StrParams>>() {
                        @Override
                        public void call(Iterator<StrParams> iter) throws Exception {
                            RFieldPutter putter = new RFieldPutter(Tables.table(Tables.PH_WBUSER_TBL));
                            putter.setCacheSize(1000);
                            while (iter.hasNext()) {
                                StrParams p = iter.next();
//                                int wr = putter.batchWrite(p);
                            }
//                            putter.flush();
                        }
                    })
            ;
            System.out.println("Total affected user size: " + accu.value());
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        } finally {
            jsc.stop();
            jsc.close();
        }
    }


}
