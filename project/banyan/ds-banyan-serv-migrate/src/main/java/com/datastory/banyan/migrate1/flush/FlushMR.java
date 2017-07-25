package com.datastory.banyan.migrate1.flush;

import com.datastory.banyan.migrate1.mapreduce.MultiTableSnapshotInputFormat;
import com.datastory.banyan.redis.RedisDao;
import com.datastory.commons3.es.lucene_writer.DefaultSettings;
import com.datastory.commons3.es.lucene_writer.FastJsonSerializer;
import com.datastory.commons3.es.lucene_writer.LocalIndexService;
import com.datastory.commons3.es.lucene_writer.ShardLocation;
import com.yeezhao.commons.util.AdvCli;
import com.yeezhao.commons.util.CliRunner;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.serialize.GsonSerializer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.MultiTableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.Logger;
import org.elasticsearch.common.Strings;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/**
 * com.datastory.banyan.migrate1.flush.FlushMR
 *
 * @author lhfcws
 * @since 2017/6/13
 */
public class FlushMR implements CliRunner {
    public static FsPermission PERM_777 = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
    public static boolean SNAPSHOT_MODE = false;
    public static Logger LOG = Logger.getLogger(FlushMR.class);
    public static String META_KEY = "banyan:etl1:flush:";
    public static String HDFS_DATA_ROOT = "/tmp/banyan/flush/";
    //    public static final String REDIS_SERVER_INFO = RhinoETLConfig.getInstance().get(RedisConsts.PARAM_REDIS_SERVER);
//    public static final String REDIS_SERVER_INFO = META_KEY + "$datatub1:6379:0";
                public static final String REDIS_SERVER_INFO = META_KEY + "$alps36:6379:0";
    static String PREFIX = "datastory.";
    static byte[] R = "r".getBytes();
    static byte[] F = "f".getBytes();

    String host;
    String index;
    FlushParams flushParams = new FlushParams();

    public static Configuration getConf() {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        conf.addResource("yarn-site.xml");
        conf.addResource("mapred-site.xml");
        conf.addResource("hdfs-site.xml");
        conf.addResource("hbase-site.xml");
        return conf;
    }

    public Scan buildScan() {
        Scan scan = new Scan();
        scan.setCaching(100);
        scan.setCacheBlocks(false);
        return scan;
    }

    public Scan buildScan(String table) {
        Scan scan = new Scan();
        scan.setCaching(100);
        scan.setCacheBlocks(false);
        scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, table.getBytes());
        return scan;
    }

    /**
     * 运行MR
     *
     * @param configs
     * @throws IOException
     */
    public void run(String jobName, StrParams configs) throws Exception {
        Job job = Job.getInstance(getConf());
        job.setJobName("FlushMR: " + jobName);
        job.setReducerClass(FlushReducer.class);
        Configuration conf = job.getConfiguration();
        job.setJarByClass(this.getClass());
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        ShardLocation shardLocation = new ShardLocation(host, index);
        shardLocation.refreshShards();
        LOG.info("[ShardLocation] " + shardLocation);

        configs.put("http.url", host);
        String mappingJson = LocalIndexService.requestMapping(configs, index);
        LOG.info("[Mapping] " + mappingJson);

        conf.set(PREFIX + "mappingJson", mappingJson);
        conf.set(PREFIX + "flushParams", GsonSerializer.serialize(flushParams));
        conf.set(PREFIX + "host", host);
        conf.set(PREFIX + "index", index);
        conf.set(PREFIX + "shardLocation", GsonSerializer.serialize(shardLocation));
        conf.set("dfs.replication", "2");
        conf.set("hbase.regionserver.codecs", "snappy");
        conf.setLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY, 300000);
        conf.set("mapreduce.job.user.classpath.first", "true");
        conf.set("mapreduce.job.running.map.limit", "150");
        conf.setInt("mapreduce.job.running.reduce.limit", conf.getInt("mapreduce.reduce.num", 48));
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.reduce.memory.mb", "8192");
        conf.set("mapreduce.reduce.java.opts", "-Xmx7168m");
        conf.setBoolean("yarn.nodemanager.vmem-check-enabled", false);
//        conf.set("mapreduce.reduce.failures.maxpercent", "1.0");     // 找空闲存储的机器时可能会fail好多次，所以设置为 1.0，全部失败才算失败
        conf.setInt("mapreduce.reduce.max.attempts", conf.getInt("mapreduce.reduce.num", 48));           // 扩大重试次数
        conf.set("mapreduce.job.maxtaskfailures.per.tracker", "1");  // fail了立即换一台TaskTracker
        conf.set("mapreduce.map.java.opts", "-Djava.library.path=/usr/hdp/current/hadoop-client/lib/native -Xmx4505m ");


        if (configs != null) {
            for (Map.Entry<String, String> e : configs.entrySet()) {
                conf.set(e.getKey(), e.getValue());
            }
        }
        job.setNumReduceTasks(conf.getInt("mapreduce.reduce.num", 48));

        // for snapshot
        final Path restoreDir = new Path("/tmp/flush/" + Strings.randomBase64UUID());
        if (!flushParams.isEmpty()) {
            Map<String, Collection<Scan>> scans = new HashMap<>();
            ArrayList<Scan> scanList = new ArrayList<>();
            for (FlushParam fp : flushParams.values()) {
                Scan scan = buildScan(fp.getHbTable());
                scans.put(fp.getHbTable(), Arrays.asList(scan));
                scanList.add(scan);
            }
            LOG.info("[SCAN] " + scans);

            if (!SNAPSHOT_MODE) {
                job.setInputFormatClass(MultiTableInputFormat.class);
                TableMapReduceUtil.initTableMapperJob(scanList, RoutingMapper.class, IntWritable.class, BytesWritable.class, job);
            } else {
                // snapshot
                FileSystem fs = FileSystem.get(getConf());
                fs.mkdirs(restoreDir);
                fs.setPermission(restoreDir, PERM_777);
                job.setInputFormatClass(MultiTableSnapshotInputFormat.class);
                MultiTableSnapshotInputFormat.initMultiTableSnapshotMapperJob(
                        scans, RoutingMapper.class, IntWritable.class, BytesWritable.class, job,
                        true, restoreDir
                );
            }
        } else {
            throw new Exception("Params are invalid.");
        }

        LOG.info("[RUN] Begin to run MR job");
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                if (SNAPSHOT_MODE) {
                    FileSystem fs = null;
                    try {
                        fs = FileSystem.get(getConf());
                        fs.delete(restoreDir);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }));
        try {
            job.waitForCompletion(true);
        } finally {
            if (SNAPSHOT_MODE) {
                FileSystem fs = FileSystem.get(getConf());
                fs.delete(restoreDir);
            }
        }
    }

    @Override
    public Options initOptions() {
        Options options = new Options();
        options.addOption("hbTable", true, "hbase table to scan data from");
        options.addOption("index", true, "es index");
        options.addOption("type", true, "es type");
        options.addOption("host", true, "es host to get mapping");
        options.addOption("yml", true, "elasticsearch.yml");
        options.addOption("conf", true, "configuration, blank split format: local.data.paths=/cloud/data1,/cloud/data2 min.free.mb=200 mapreduce.reduce.memory.mb=2048");

        return options;
    }

    @Override
    public boolean validateOptions(CommandLine cmd) {
        LOG.info("[Args] " + Arrays.toString(cmd.getArgs()));
        return cmd.hasOption("hbTable")
                && cmd.hasOption("index")
                && cmd.hasOption("type")
                && cmd.hasOption("host")
                && cmd.hasOption("yml")
                ;
    }

    @Override
    public void start(CommandLine cmd) {
        try {
            // parse
            String jobName = parseParams(cmd);

            // 加载 elasticsearch.yml 配置文件，用于生成EsLuceneWriter
            String yml = cmd.getOptionValue("yml");
            Map esYml = DefaultSettings.loadElasticSearchYml(new FileInputStream(yml));
            StrParams esYmlStrMap = new StrParams();
            for (Object key : esYml.keySet()) {
                esYmlStrMap.put(key.toString(), esYml.get(key).toString());
            }

            String json = FastJsonSerializer.serializePretty(esYmlStrMap);
            LOG.info("[elasticsearch.yml] \n" + json);

            StrParams conf = new StrParams("elasticsearch.yml", json);
            if (cmd.hasOption("conf")) {
                String[] confArr = cmd.getOptionValues("conf");
                LOG.info("[conf] " + Arrays.toString(confArr));
                for (String item : confArr) {
                    if (item.trim().isEmpty())
                        continue;
                    String[] itemArr = item.split("=");
                    conf.put(itemArr[0], itemArr[1]);
                }
            }

            // 初始化redis，用于分布式记录 路径和host的信息
            LOG.info("[INIT] Init redis : delete key " + META_KEY + index);
            RedisDao redisDao = RedisDao.getInstance(REDIS_SERVER_INFO);
            redisDao.del(META_KEY + index);

            // 初始化用于上传的HDFS
            LOG.info("[INIT] Init HDFS : empty directory : " + HDFS_DATA_ROOT + index);
            FileSystem fs = FileSystem.get(getConf());
            fs.delete(new Path(HDFS_DATA_ROOT + index));
            fs.mkdirs(new Path(HDFS_DATA_ROOT + index));

            // 运行MR
            run(jobName, conf);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            cleanup();
        }
    }

    protected String parseParams(CommandLine cmd) throws Exception {
        this.host = cmd.getOptionValue("host");
        this.index = cmd.getOptionValue("index");
        String[] hbTable = cmd.getOptionValue("hbTable").split(",");
        String[] type = cmd.getOptionValue("type").split(",");

        int len = hbTable.length;

        for (int i = 0; i < len; i++) {
            String t = hbTable[i].trim();
            String snapshotTable = snapshot(t);
            flushParams.put(t, new FlushParam(
                    type[i].trim(), snapshotTable, t
            ));
        }
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                cleanup();
            }
        }));

        return cmd.getOptionValue("hbTable") + " -> " + cmd.getOptionValue("index");
    }

    protected synchronized void cleanup() {
        LOG.info("[RUN] Begin to cleanup .");
        if (SNAPSHOT_MODE) {
            for (FlushParam fp : flushParams.values()) {
                try {
                    dropSnapshot(fp.getHbTable());
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
        flushParams.clear();
    }

    /**
     * Use hbase snapshot to guarantee the data consistence and accelerate the scan speed.
     *
     * @param hbTable
     * @return
     */
    protected synchronized String snapshot(String hbTable) throws IOException {
        if (SNAPSHOT_MODE) {
            String suffix = "_flush_" + System.currentTimeMillis();
            String snapshotName = hbTable + suffix;

            HBaseAdmin admin = null;
            Connection conn = null;
            try {
                Configuration conf = getConf();
                LOG.info("zookeeper.znode.parent=" + conf.get("zookeeper.znode.parent"));
                conn = ConnectionFactory.createConnection(conf);
                admin = new HBaseAdmin(conn);
                admin.snapshot(snapshotName.getBytes(), hbTable.getBytes());
            } finally {
                if (admin != null)
                    admin.close();
                if (conn != null)
                    conn.close();
            }

            return snapshotName;
        } else {
            return hbTable;
        }
    }

    protected void dropSnapshot(String snapshotTable) throws IOException {
        if (SNAPSHOT_MODE) {
            HBaseAdmin admin = null;
            Connection conn = null;
            try {
                conn = ConnectionFactory.createConnection(getConf());
                admin = new HBaseAdmin(conn);
                admin = new HBaseAdmin(getConf());
                admin.deleteSnapshot(snapshotTable);
                LOG.info("[SNAPSHOT] Drop snapshot: " + snapshotTable);
            } finally {
                if (admin != null)
                    admin.close();
                if (conn != null)
                    conn.close();
            }
        }
    }

    /**
     * AdvCli main
     *
     * @param args
     */
    public static void main(String[] args) {
        System.out.println("[PROGRAM] Program started.");
        AdvCli.initRunner(args, FlushMR.class.getSimpleName(), new FlushMR());
        System.out.println("[PROGRAM] Program exited.");
    }
}