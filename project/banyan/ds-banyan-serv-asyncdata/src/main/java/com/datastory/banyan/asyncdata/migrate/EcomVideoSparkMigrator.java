package com.datastory.banyan.asyncdata.migrate;

import com.datastory.banyan.asyncdata.migrate.doc.EcomVideoMigrateDocMapper;
import com.datastory.banyan.asyncdata.util.RConfig;
import com.datastory.banyan.asyncdata.util.SiteTypes;
import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.doc.DocMapper;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.spark.SparkUtil;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.util.Iterator;

/**
 * com.datastory.banyan.asyncdata.migrate.EcomVideoSparkMigrator
 *
 * @author lhfcws
 * @since 2017/4/21
 */
public class EcomVideoSparkMigrator implements Serializable {
    protected transient Configuration srcConf;
    protected String srcTable;
    protected String targetTable;
    protected String docMapperClass;
    protected int cores = 100;

    public EcomVideoSparkMigrator(Configuration srcConf, String srcTable, String targetTable, String docMapperClass) {
        this.srcConf = srcConf;
        this.srcTable = srcTable;
        this.targetTable = targetTable;
        this.docMapperClass = docMapperClass;
    }

    public Scan buildAllScan(Filter filter) {
        Scan scan = HBaseUtils.buildScan();
        if (filter != null)
            scan.setFilter(filter);
        return scan;
    }

    public Scan buildAllScan() {
        return buildAllScan(null);
    }

    public void run(Scan scan) throws IOException {
        if (scan == null)
            scan = buildAllScan();

        String appName = this.getClass().getSimpleName() + ": " + srcTable + "-" + targetTable;
        JavaSparkContext jsc = SparkUtil.createSparkContext(this.getClass(), appName, cores);
        try {
            srcConf.set(TableInputFormat.SCAN, HBaseUtils.convertScanToString(scan));
            srcConf.set(TableInputFormat.INPUT_TABLE, srcTable);
            final Accumulator<Integer> totalAcc = jsc.accumulator(0);
            final Accumulator<Integer> writeAcc = jsc.accumulator(0);
            JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD
                    = jsc.newAPIHadoopRDD(srcConf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
            hBaseRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>>() {
                @Override
                public void call(Iterator<Tuple2<ImmutableBytesWritable, Result>> iter) throws Exception {
                    RFieldPutter writer = new RFieldPutter(targetTable);
                    while (iter.hasNext()) {
                        Tuple2<ImmutableBytesWritable, Result> tpl2 = iter.next();
                        Result result = tpl2._2();
                        totalAcc.add(1);
                        boolean pass = filter(result);
                        if (!pass)
                            continue;
                        DocMapper docMapper = buildDocMapper(result);
                        Params p = (Params) docMapper.map();
                        if (p != null) {
                            writer.batchWrite(p);
                            writeAcc.add(1);
                        }
                    }
                    writer.flush();
                }
            });

            System.out.println("\n[TOTAL] " + totalAcc.value());
            System.out.println("[WRITE] " + writeAcc.value());
        } finally {
            jsc.stop();
            jsc.close();
        }
    }

    protected boolean filter(Result result) throws IOException {
        String siteName = HBaseUtils.getValue(result, "raw".getBytes(), "site".getBytes());
        String siteType = SiteTypes.getInstance().getType(siteName);
        if (SiteTypes.T_ECOM.equals(siteType) && targetTable.contains("ECOM")) {
            return true;
        } else if (SiteTypes.T_VIDEO.equals(siteType) && targetTable.contains("VIDEO")) {
            return true;
        } else
            return false;
    }

    protected DocMapper buildDocMapper(Result result) throws Exception {
        if (docMapperClass.equals(EcomVideoMigrateDocMapper.class.getCanonicalName())) {
            // accelerate : new instead of reflection
            return new EcomVideoMigrateDocMapper(srcTable, result);
        } else {
            Class<? extends DocMapper> klass = (Class<? extends DocMapper>) Class.forName(docMapperClass);
            Constructor<? extends DocMapper> constructor = klass.getConstructor(Result.class);
            return constructor.newInstance(result);
        }
    }

    /***********************
     * main
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String srcTable = args[0];
        String targetTable = args[1];
        String docMapperCN = BanyanTypeUtil.safeGet(args, 2, EcomVideoMigrateDocMapper.class.getCanonicalName());
        String srcConfCN = BanyanTypeUtil.safeGet(args, 3, RConfig.class.getCanonicalName());

        Configuration srcConf = null;
        if (srcConfCN.equals(RConfig.class.getCanonicalName())) {
            srcConf = RConfig.getInstance();
        } else if (srcConfCN.equals(RhinoETLConfig.class.getCanonicalName())) {
            srcConf = RhinoETLConfig.getInstance();
        }

        EcomVideoSparkMigrator migrator = new EcomVideoSparkMigrator(srcConf, srcTable, targetTable, docMapperCN);
        migrator.run(migrator.buildAllScan());
    }
}
