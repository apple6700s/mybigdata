package com.datastory.banyan.migrate1.flush;

import com.datastory.banyan.redis.RedisDao;
import com.datastory.banyan.spark.ScanFlushESMR;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.commons3.es.lucene_writer.*;
import com.google.common.base.Function;
import com.xiaoleilu.hutool.util.FileUtil;
import com.xiaoleilu.hutool.util.ZipUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.Entity.StrParams;
import com.yeezhao.commons.util.RuntimeUtil;
import com.yeezhao.commons.util.exec.CommandExecutor;
import com.yeezhao.commons.util.serialize.GsonSerializer;
import com.yeezhao.commons.util.serialize.ProtostuffSerializer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.lucene.index.IndexWriterConfig;
import org.elasticsearch.action.index.IndexRequest;
import org.jsoup.helper.StringUtil;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static com.datastory.banyan.migrate1.flush.FlushMR.*;

/**
 * com.datastory.banyan.migrate1.flush.FlushReducer
 *
 * @author lhfcws
 * @since 2017/6/16
 */
public class FlushReducer extends Reducer<IntWritable, BytesWritable, NullWritable, NullWritable> {
    public static Logger LOG = Logger.getLogger(FlushReducer.class);
    public static final int MAX_BUFFER_SIZE = 10000;
    public static final int MAX_WRITE_SIZE_PER_WRITER = 100000;      // 10w
    String host;
    String index;
    FlushParams flushParams = null;

    long minFreeMb = 204800l;    // dft 200g
    String[] localDataRoot;

    StrParams elasticsearchYml;
    ShardLocation shardLocation;
    LocalIndexWriterFactory factory = new LocalIndexWriterFactory();
    String mappingJson;

    private LocalIndexWriter createWriter(EsShard shard, StrParams esConf) throws IOException {
        Function<IndexWriterConfig, IndexWriterConfig> iwcFunc = new Function<IndexWriterConfig, IndexWriterConfig>() {
            @Nullable
            @Override
            public IndexWriterConfig apply(@Nullable IndexWriterConfig iwc) {
                iwc.setCommitOnClose(true);
                iwc.setRAMBufferSizeMB(50);
                return iwc;
            }
        };

        LocalIndexWriter writer = factory.createLocalIndexWriter(index, shard.getShardId(), esConf, iwcFunc);
        String translogId = TranslogIniter.genUuid(shard);
        HashMap<String, String> userData = new HashMap<String, String>();
        userData.put("translog_uuid", translogId);
        userData.put("translog_generation", "1");
        writer.setData(userData);
        return writer;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        try {
            // 初始化变量
            Configuration conf = context.getConfiguration();
            host = conf.get(PREFIX + "host");
            index = conf.get(PREFIX + "index");
            String fpJson = conf.get(PREFIX + "flushParams");
            this.flushParams = GsonSerializer.deserialize(fpJson, FlushParams.class);

            // 从配置初始化
            localDataRoot = conf.getStrings("local.data.paths");
            minFreeMb = conf.getLong("min.free.mb", 204800l);
            String ymlJson = conf.get("elasticsearch.yml");
            elasticsearchYml = FastJsonSerializer.deserialize(ymlJson, StrParams.class);
            System.getProperties().put("path.home", "/tmp");
            mappingJson = conf.get(PREFIX + "mappingJson");

            // 初始化 shard 的相关信息
            // 初始化 shard 的相关信息
            String slJson = conf.get(PREFIX + "shardLocation");
            shardLocation = GsonSerializer.deserialize(slJson, ShardLocation.class);
            if (shardLocation.getShardNum() == 0)
                throw new InterruptedException("Cannot get shard informations");

            LOG.info("[ShardLocation] " + shardLocation);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    @Override
    protected void reduce(IntWritable shardIdWritable, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
        // 获取当前的shard
        Integer shardId = shardIdWritable.get();
        EsShard shard = shardLocation.getShard(shardId);
        int retry = 10;
        while (shard == null && shardLocation.getShardNum() == 0 && retry-- >= 0) {
            try {
                synchronized (shardLocation) {
                    if (shardLocation.getShardNum() == 0)
                        shardLocation.refreshShards();
                }
            } catch (Exception e) {
                LOG.warn(e.getMessage(), e);
            }
        }
        if (shardLocation.getShardNum() == 0)
            throw new InterruptedException("Cannot get shard informations");
        LOG.info("[BEGIN] " + index + " , " + shard);

        // 迭代可用目录，检查空余存储
        String dir = getFreeDir();
        LOG.info("[DIR] Free dir on " + RuntimeUtil.Hostname + " : " + dir);
        if (dir == null) {
            LOG.error("[DIR] No enough free space on " + RuntimeUtil.Hostname);
            LOG.error("[DIR] Try re-run it on another machine.");
            context.getCounter(ScanFlushESMR.ROW.SWITCH).increment(1);
            // 存储不足，抛出换台机器重试
            throw new InterruptedException("[DIR] No enough free space on " + RuntimeUtil.Hostname);
        }

        // 生成本地可写目录 host/nodeName/index/shardId
        String subDir = shard.getIndex() + "/" + shard.getHost() + "/" + shard.getNodeName() + "/" + shardId;
        File dirFile = new File(dir);
        if (!dirFile.canWrite()) {
            InterruptedException e = new InterruptedException(String.format("[DIR] Directory [%s] on [%s] is not writable. Plz check the permission.", dir, RuntimeUtil.Hostname));
            LOG.error(e.getMessage(), e);
            context.getCounter(ScanFlushESMR.ROW.SWITCH).increment(1);
            throw e;
        } else {
            dir = dir + "/" + subDir;
            dirFile = new File(dir);
            if (dirFile.exists())
                FileUtil.del(dirFile);
            dirFile.mkdirs();
        }

        final File dirFile_ = dirFile;
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    boolean isDeleted = FileUtil.del(dirFile_);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }));

        LOG.info("[DIR] Gen local index data dir : " + dir);

        StrParams esConf = new StrParams(elasticsearchYml);
        esConf.put("local.path.data", dir);
        esConf.put("mapping.json." + index, mappingJson);

        try {
            // 初始化LuceneWriter， 迭代数据写索引
            LocalIndexWriter writer = createWriter(shard, esConf);
//            long totalCnt = 0;
            long cnt = 0;
            try {
                LOG.info("[Writer] Initialized writer for " + index + " , shard " + shardId);
                List<IndexRequest> buffer = new LinkedList<>();
                for (BytesWritable bytesWritable : values) {
                    byte[] bytes = bytesWritable.copyBytes();
                    // bug: https://groups.google.com/forum/#!topic/protobuf/lwdsWd5c8g0
                    Params.ParamsPojo pojo = ProtostuffSerializer.deserialize(bytes, Params.ParamsPojo.class);
                    Params esDoc = pojo.getP();
                    if (esDoc != null && !esDoc.isEmpty()) {
                        String esType = (String) esDoc.remove("_type");
                        if  (esDoc.getString("id") == null) {
                            context.getCounter(ScanFlushESMR.ROW.NO_ID).increment(1);
                            continue;
                        }
                        IndexRequest indexRequest = toIndexRequest(esDoc, esType);
                        buffer.add(indexRequest);
                        cnt++;
                        if (buffer.size() >= MAX_BUFFER_SIZE) {
                            writer.write(buffer);
                            context.getCounter(ScanFlushESMR.ROW.WRITE).increment(buffer.size());
                            buffer.clear();
                        }

                        if (cnt >= MAX_WRITE_SIZE_PER_WRITER) {
//                            cnt = 0;
//                            writer.flush();
//                            writer.close();
//                            LOG.info("[Writer] Reinited writer for " + index + " , shard " + shardId);

//                            writer = createWriter(shard, esConf);
                        }
                    }
                }

                if (!buffer.isEmpty()) {
                    writer.write(buffer);
                    context.getCounter(ScanFlushESMR.ROW.WRITE).increment(buffer.size());
                    buffer.clear();
                }
            } finally {
                writer.flush();
                writer.close();
                LOG.info("[Writer] Closed writer for [" + index + "] , shard " + shardId + ", TOTAL DOC COUNT = " + cnt);
            }

            // zip压缩，并上传HDFS
            printListFile(dir);
            String hdfsDir = HDFS_DATA_ROOT + subDir;
            LOG.info("[Upload] HDFS : " + hdfsDir);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path hdfsDirPath = new Path(hdfsDir);
            fs.mkdirs(hdfsDirPath);
            String zip = compressLocalDir(dir, shardId);
            Path zipPath = new Path(zip);
            fs.copyFromLocalFile(true, zipPath, hdfsDirPath);
            LOG.info("[Upload] ZIP : " + zip);
            LOG.info("[DONE] " + index + " , " + shard);

            // 将路径元信息记录Redis
            RedisDao redisDao = RedisDao.getInstance(REDIS_SERVER_INFO);
            try {
                StrParams dirMetaData = new StrParams();
                dirMetaData.put(shardId + ".host", shard.getHost());
                dirMetaData.put(shardId + ".worker_host", RuntimeUtil.getHostname());
                dirMetaData.put(shardId + ".subDir", subDir);
                dirMetaData.put(shardId + ".localDir", dir);
                dirMetaData.put(shardId + ".hdfsDir", hdfsDir);

                String metaDataJson = GsonSerializer.serialize(dirMetaData);
                redisDao.hset(META_KEY + index, "" + shardId, metaDataJson);
                LOG.info("[Redis] hset " + META_KEY + index + " : " + shardId + " = " + metaDataJson);
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        } finally {
            // 删除本地的数据
            boolean isDeleted = FileUtil.del(dirFile);
            LOG.info("[DIR] " + isDeleted + " , delete local dir : " + dir);
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }

    // ========== utils

    protected String getFreeDir() {
        for (String dir : BanyanTypeUtil.shuffleArray(localDataRoot)) {
            if (checkFreeSpace(dir)) {
                return dir;
            }
        }
        return null;
    }

    protected boolean checkFreeSpace(String dir) {
        File file = new File(dir);
        if (!file.exists())
            file.mkdirs();

        long bytes = file.getFreeSpace();
        long mb = bytes / 1024 / 1024;
        LOG.info("[DIR] Check space : " + dir + " , free mb: " + mb);
        return (mb >= minFreeMb);
    }


    protected IndexRequest toIndexRequest(Params esDoc, String type) {
        IndexRequest indexRequest = new IndexRequest();
        indexRequest.index(index).type(type);
        String id = esDoc.getString("id");
        indexRequest.id(id);
        if (esDoc.containsKey("_parent")) {
            String parent = (String) esDoc.remove("_parent");
            indexRequest.parent(parent);
        }
        indexRequest.timestamp(System.currentTimeMillis() + "");
        indexRequest.source(esDoc);
        return indexRequest;
    }

    /**
     * 压缩一个本地目录
     *
     * @param dir
     * @param shardId
     * @return
     * @throws IOException
     */
    protected String compressLocalDir(String dir, int shardId) throws IOException {
        String destDir = dir;
        File dest = new File(destDir);
        destDir = dest.getParent();
        destDir = destDir + "/" + shardId + ".zip";
        dest = new File(destDir);
        File src = new File(dir);
        LOG.info("[ZIP] Zip : " + src.getAbsolutePath() + "  ->  " + dest.getAbsolutePath());
        ZipUtil.zip(src, dest, false);
        src.delete();
        return destDir;
    }

    protected void printListFile(String dir) {
        System.out.println("============================");
        System.out.println(StringUtil.join(Arrays.asList("du", "-sh", dir), " "));
        System.out.println();
        CommandExecutor commandExecutor1 = new CommandExecutor(Arrays.asList("du", "-sh", dir));
        commandExecutor1.run(System.out);
        System.out.println("============================");

        System.out.println(StringUtil.join(Arrays.asList("ls", "-l", dir), " "));
        System.out.println();
        CommandExecutor commandExecutor2 = new CommandExecutor(Arrays.asList("ls", "-l", dir));
        commandExecutor2.run(System.out);
        System.out.println("============================");
    }
}
