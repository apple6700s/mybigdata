package com.datastory.banyan.migrate1;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.HTableInterfacePool;
import com.datastory.banyan.hbase.PhoenixIndexWriter;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.hbase.hooks.*;
import com.datastory.banyan.monitor.stat.AccuStat;
import com.datastory.banyan.spark.AbstractSparkRunner;
import com.datastory.banyan.spark.SparkUtil;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.ErrorUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.encypt.Md5Util;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * com.datastory.banyan.hbase.migrate.spark.Hb2PhMigrateSparkTemplate
 *
 * @author lhfcws
 * @since 16/11/21
 */
public class Hb2PhMigrateSparkTemplate extends AbstractSparkRunner {
    protected static Logger LOG = Logger.getLogger(Hb2PhMigrateSparkTemplate.class);

    protected String srcTable;
    protected String targetTable;
    protected String pkField;
    protected String[] idxFields;
    protected String[] srcFields;
    protected String[] targetFields;
    protected byte[][] families = {"r".getBytes()};
    protected Set<String> fieldSet = new HashSet<>();

    protected int cores = 400;

    @Override
    public int getCores() {
        return cores;
    }

    @Override
    public Hb2PhMigrateSparkTemplate setCores(int cores) {
        this.cores = cores;
        return this;
    }

    public Params customizedValue(Params p) {
        return p;
    }

    public Hb2PhMigrateSparkTemplate(String srcTable, String targetTable, String pkField, String[] idxFields, String[] srcFields, String[] targetFields) {
        this.srcTable = srcTable;
        this.targetTable = targetTable;
        this.idxFields = idxFields;
        this.pkField = pkField;
        this.srcFields = srcFields;
        this.targetFields = targetFields;

        fieldSet.addAll(Arrays.asList(this.targetFields));
    }

    public String genPK(String s) {
        return Md5Util.md5(s).substring(0, 3) + s;
    }

    public void run() throws IOException {
        run(null);
    }

    public Scan buildScan() {
        return HBaseUtils.buildScan();
    }

    public Scan buildOneScan() {
        Scan scan = HBaseUtils.buildScan();
        scan.setStartRow("191774089471".getBytes());
        scan.setStopRow("191774089472".getBytes());
        return scan;
    }

    public Params filterParams(Params p) {
        return p;
    }

    public Configuration getSrcConf() {
        return new Configuration(RhinoETLConfig.getInstance());
    }

    public void run(Filter filter) throws IOException {
        Scan scan = buildScan();
        if (filter != null) {
            scan.setFilter(filter);
        }
        System.out.println("[SCAN] " + scan);

        StrParams sparkConf = new StrParams();
        sparkConf.put("spark.executor.memory", "3g");
        sparkConf.put("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.put("spark.kryo.registrator", "com.datastory.banyan.spark.BanyanKryoRegister");

        Configuration srcConf = getSrcConf();
        srcConf.set(TableInputFormat.SCAN, HBaseUtils.convertScanToString(scan));
        srcConf.set(TableInputFormat.INPUT_TABLE, srcTable);
        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), false, this.targetTable, getCores() + "", sparkConf);
        final Accumulator<Integer> allCounter = jsc.accumulator(0, "all");
        final Accumulator<Integer> flushCounter = jsc.accumulator(0, "flush");
        try {
            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD =
                    jsc.newAPIHadoopRDD(srcConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            hBaseRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Params>() {
                @Override
                public Params call(Tuple2<ImmutableBytesWritable, Result> tpl2) throws Exception {
                    Result result = tpl2._2();
                    Params ret = new Params();
                    for (byte[] family : families) {
                        Map<byte[], byte[]> bm = result.getFamilyMap(family);
                        if (bm != null && !bm.isEmpty()) {
                            StrParams p = new StrParams(BanyanTypeUtil.byte2strMap(bm));
                            ret.putAll(p);
                        }
                    }
                    return ret;
                }
            }).repartition(getCores())
                    .foreachPartition(new VoidFunction<Iterator<Params>>() {
                        @Override
                        public void call(Iterator<Params> paramsIterator) throws Exception {
                            RFieldPutter putter = new RFieldPutter(targetTable);
                            putter.getHooks().add(new HBaseMain2Hook(new HBaseRetryLog2Hook(targetTable)));
                            int size = 0;

                            AccuStat stat = new AccuStat();
                            while (paramsIterator.hasNext()) {
                                try {
                                    allCounter.add(1);
                                    Params p = paramsIterator.next();
                                    if (p == null) continue;
                                    p = filterParams(p);
                                    if (p == null || p.isEmpty() || p.get(pkField) == null) continue;

                                    p.remove("_0");
                                    String pk = genPK(p.getString(pkField));
                                    if (pk == null) continue;

                                    Params idx = new Params();
                                    idx.put("pk", pk);
                                    for (String idxField : idxFields) {
                                        if (idxField.equals("pk")) continue;

                                        String[] arr = idxField.split("\\.");
                                        if (arr.length == 2) {
                                            if (p.get(arr[0]) != null)
                                                idx.put(arr[1], p.remove(arr[0]));
                                        } else {
                                            if (p.get(idxField) != null)
                                                idx.put(idxField, p.remove(idxField));
                                        }
                                    }

                                    for (int i = 0; i < srcFields.length; i++) {
                                        String srcField = srcFields[i];
                                        String targetField = targetFields[i];

                                        if (!srcField.equals(targetField) && p.get(srcField) != null) {
                                            p.put(targetField, p.remove(srcField));
                                        }
                                    }

                                    size++;
                                    List<String> keys = new LinkedList<String>(p.keySet());
                                    for (String key : keys) {
                                        if (!fieldSet.contains(key))
                                            p.remove(key);
                                    }

                                    p.put("pk", pk);
                                    p.putAll(idx);
                                    p = customizedValue(p);
                                    flushCounter.add(1);

                                    int wr = putter.batchWrite(p);

                                } catch (Exception e) {
                                    if (e.getStackTrace().length > 0) {
                                        LOG.error(ErrorUtil.e2s(e));
                                    } else {
                                        LOG.error(e.getMessage() + " : empty exception??");
                                    }
                                }
                            }
                            // end iter
                            putter.flush(true);
                            System.out.println("partition size: " + size + ", avg cost(ms): " + stat.getAvg(size));
                        }
                    });
        } finally {
            System.out.println("\nallCounter: " + allCounter.value() + ", flushCounter: " + flushCounter.value());
            jsc.stop();
            jsc.close();
        }
    }

    public String getSrcTable() {
        return srcTable;
    }

    public void setSrcTable(String srcTable) {
        this.srcTable = srcTable;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

    public String[] getSrcFields() {
        return srcFields;
    }

    public void setSrcFields(String[] srcFields) {
        this.srcFields = srcFields;
    }

    public String[] getTargetFields() {
        return targetFields;
    }

    public void setTargetFields(String[] targetFields) {
        this.targetFields = targetFields;
    }

    public byte[][] getFamilies() {
        return families;
    }

    public void setFamilies(byte[][] families) {
        this.families = families;
    }
}
