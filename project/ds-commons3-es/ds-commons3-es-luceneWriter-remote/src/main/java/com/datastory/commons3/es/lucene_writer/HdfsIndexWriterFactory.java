package com.datastory.commons3.es.lucene_writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * com.datastory.commons3.es.lucene_writer.HdfsIndexWriterFactory
 *
 * @author lhfcws
 * @since 2017/5/4
 */
public class HdfsIndexWriterFactory {
    public HdfsIndexWriter createHdfsIndexWriter(String indexName, int shardId, Map<String, String> esConfigs, Configuration conf) throws IOException {
        DefaultSettings.initEmptyPathHome();

        esConfigs.put("lucene.hdfs.enable", "true");

        HdfsIndexWriter indexWriter = new HdfsIndexWriter();
        indexWriter.indexService = new HdfsIndexServiceProvider().create(indexName, shardId, esConfigs, conf);
        indexWriter.logger = Loggers.getLogger(Engine.class,
                // we use the engine class directly here to make sure all subclasses have the same logger name
                indexWriter.indexService.settings, new ShardId(new Index(indexName), shardId));
        indexWriter.directory = indexWriter.indexService.getLuceneDirectory();
        indexWriter.indexWriter = indexWriter.createWriter();

        return indexWriter;
    }

    public void setTranslogUuid(IndexWriter writer, String tryUuid) throws IOException {
        HashMap<String, String> userData = new HashMap<>();
        userData.put(TranslogIniter.TRANSLOG_UUID, tryUuid);
        userData.put(TranslogIniter.TRANSLOG_GENERATION, "1");
        writer.setCommitData(userData);
    }

    public boolean tryInitTranslog(LocalIndexWriter writer, String tryUuid) throws IOException {
        boolean canTry = !writer.existsDirectory();
        if (canTry) {
            HashMap<String, String> userData = new HashMap<>();
            userData.put(TranslogIniter.TRANSLOG_UUID, tryUuid);
            userData.put(TranslogIniter.TRANSLOG_GENERATION, "1");
            writer.setData(userData);
        }
        return canTry;
    }

    public boolean tryInitTranslog(HdfsIndexWriter writer, String pathIndexData, String tryUuid) throws IOException {
        boolean canTry = !writer.existsDirectory();
        if (canTry) {
            long generation = 1;
            FileSystem fs = FileSystem.get(writer.getConf());

            TranslogIniter translogIniter = new TranslogIniter();
            String translogUUid = tryUuid;
            Path tlogPath = new Path(translogIniter.getPath(pathIndexData));
            fs.mkdirs(tlogPath.getParent());
            FSDataOutputStream os = fs.create(tlogPath);
            int tlogSize = translogIniter.write(os, translogUUid);

            String ckpPath = translogIniter.getCkpPath(pathIndexData);
            FSDataOutputStream ckpOs = fs.create(new Path(ckpPath));
            translogIniter.writeInitCheckpoint(ckpOs, tlogSize, generation);

            HashMap<String, String> userData = new HashMap<>();
            userData.put(TranslogIniter.TRANSLOG_UUID, translogUUid);
            userData.put(TranslogIniter.TRANSLOG_GENERATION, "1");
            writer.setData(userData);

        }

        return canTry;
    }
}
