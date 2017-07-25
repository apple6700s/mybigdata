package com.datastory.banyan.asyncdata.migrate;

import com.datastory.banyan.asyncdata.migrate.doc.EcomVideoMigrateDocMapper;
import com.datastory.banyan.asyncdata.util.RConfig;
import com.datastory.banyan.asyncdata.util.SiteTypes;
import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.doc.DocMapper;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.MRCounter;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

/**
 * com.datastory.banyan.asyncdata.migrate.EcomVideoMigrator
 *
 * @author lhfcws
 * @since 2017/4/12
 */
public class EcomVideoMigrator implements Serializable {
    protected Configuration srcConf;
    protected Configuration tagetConf = RhinoETLConfig.getInstance();
    protected String srcTable;
    protected String targetTable;
    protected Class<? extends DocMapper> migrateDocMapper;

    public EcomVideoMigrator(String srcTable, String targetTable, Class<? extends DocMapper> migrateDocMapper) {
        this(RhinoETLConfig.getInstance(), srcTable, targetTable, migrateDocMapper);
    }

    public EcomVideoMigrator(Configuration srcConf, String srcTable, String targetTable, Class<? extends DocMapper> migrateDocMapper) {
        this.srcConf = srcConf;
        this.srcTable = srcTable;
        this.targetTable = targetTable;
        this.migrateDocMapper = migrateDocMapper;
    }

    public void run() throws IOException, ClassNotFoundException, InterruptedException {
        Scan scan = buildAllScan();
        Job job = buildJob(srcTable, targetTable, scan, getMapper(), migrateDocMapper);
        job.waitForCompletion(true);
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

    public Job buildJob(String srcTable, String targetTable, Scan scan, Class<? extends TableMapper> mapperClass, Class<? extends DocMapper> migrateDocMapper) throws IOException {
        System.out.println("[SCAN] " + srcTable + ";  " + scan);
        Job job = Job.getInstance(RConfig.getInstance());
        Configuration conf = job.getConfiguration();

        job.setInputFormatClass(TableInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setJobName(this.getClass().getSimpleName() + "-" + srcTable);
        TableMapReduceUtil.initTableMapperJob(srcTable, scan, mapperClass, NullWritable.class, NullWritable.class, job);
//        TableMapReduceUtil.initTableReducerJob(
//                targetTable,      // output table
//                null,             // reducer class
//                job);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setJarByClass(this.getClass());

        job.setNumReduceTasks(0);
        conf.set("hbase.client.keyvalue.maxsize", "" + (50 * 1024 * 1024));
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapred.reduce.slowstart.completed.maps", "1.0");  // map跑完100% 才跑reducer
        conf.set("mapreduce.job.running.map.limit", "100");
        conf.set("mapreduce.reduce.memory.mb", "2048");
        conf.set("mapreduce.map.memory.mb", "2048");

        conf.set("banyan.docmapper", getMigrateDocMapper().getCanonicalName());
        conf.set("banyan.srcTable", srcTable);
        conf.set("banyan.targetTable", targetTable);

        return job;
    }

    public Configuration getSrcConf() {
        return srcConf;
    }

    public void setSrcConf(Configuration srcConf) {
        this.srcConf = srcConf;
    }

    public Configuration getTagetConf() {
        return tagetConf;
    }

    public void setTagetConf(Configuration tagetConf) {
        this.tagetConf = tagetConf;
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

    public Class<? extends DocMapper> getMigrateDocMapper() {
        return migrateDocMapper;
    }

    public void setMigrateDocMapper(Class<? extends DocMapper> migrateDocMapper) {
        this.migrateDocMapper = migrateDocMapper;
    }

    public Class<? extends TableMapper> getMapper() {
        return HbaseMapper.class;
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

        Class<? extends DocMapper> docMapperClass = (Class<? extends DocMapper>) Class.forName(docMapperCN);

        EcomVideoMigrator migrator = new EcomVideoMigrator(srcConf, srcTable, targetTable, docMapperClass);
        migrator.run();
    }

    /********************************
     * Mapper
     */
    public static class HbaseMapper extends TableMapper<ImmutableBytesWritable, Put> {
        private static final byte[] RAW = "raw".getBytes();

        protected String srcTable;
        protected String targetTable;
        protected Class<? extends DocMapper> migrateDocMapperClass;
        protected Constructor<? extends DocMapper> constructor;
        protected DocMapper _docMapper;
        protected RFieldPutter writer;


        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            try {
                srcTable = conf.get("banyan.srcTable");
                targetTable = conf.get("banyan.targetTable");
                migrateDocMapperClass = (Class<? extends DocMapper>) Class.forName(conf.get("banyan.docmapper"));
                Constructor<? extends DocMapper> constructor = migrateDocMapperClass.getConstructor(Result.class);
                _docMapper = constructor.newInstance(null);
                writer = new RFieldPutter(targetTable);
                writer.getHooks().clear();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            context.getCounter(MRCounter.MAP).increment(1);
            if (value.isEmpty()) {
                context.getCounter(MRCounter.EMPTY).increment(1);
                return;
            }

            try {
                boolean pass = filter(value);
                if (!pass)
                    return;

                DocMapper docMapper = buildDocMapper(value);
                Params p = (Params) docMapper.map();
                if (p == null)
                    return;

                ImmutableBytesWritable newKey = new ImmutableBytesWritable(p.getString("pk").getBytes());

                Put put = new Put(newKey.get());
                for (Map.Entry<String, ? extends Object> e : p.entrySet()) {
                    if (e.getValue() == null) continue;
                    String v = String.valueOf(e.getValue());
                    put.addColumn(RhinoETLConsts.R, e.getKey().getBytes(), v.getBytes());
                }

                writer.batchWrite(put);
                context.getCounter(MRCounter.WRITE).increment(1);
            } catch (Exception e) {
                context.getCounter(MRCounter.ERROR).increment(1);
                throw new IOException(e);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            writer.flush();
        }

        protected DocMapper buildDocMapper(Result value) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
            if (_docMapper instanceof EcomVideoMigrateDocMapper) {
                // accelerate : new instead of reflection
                return new EcomVideoMigrateDocMapper(srcTable, value);
            } else
                return constructor.newInstance(value);
        }

        protected boolean filter(Result result) throws IOException {
            String siteName = HBaseUtils.getValue(result, RAW, "site".getBytes());
            String siteType = SiteTypes.getInstance().getType(siteName);
            if (SiteTypes.T_ECOM.equals(siteType) || targetTable.contains("ECOM")) {
                return true;
            } else if (SiteTypes.T_VIDEO.equals(siteType) || targetTable.contains("VIDEO")) {
                return true;
            } else
                return false;
        }
    }
}
