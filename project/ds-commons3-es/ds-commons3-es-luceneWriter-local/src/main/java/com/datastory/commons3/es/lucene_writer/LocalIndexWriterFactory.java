package com.datastory.commons3.es.lucene_writer;

import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Map;

/**
 * com.datastory.commons3.es.lucene_writer.LocalIndexWriterFactory
 *
 * @author lhfcws
 * @since 2017/5/4
 */
public class LocalIndexWriterFactory {
    public LocalIndexWriter createLocalIndexWriter(String indexName, int shardId) throws IOException {
        LocalIndexWriter indexWriter = new LocalIndexWriter();
        indexWriter.indexService = new LocalIndexServiceProvider().create(indexName, shardId);
        indexWriter.logger = Loggers.getLogger(Engine.class,
                // we use the engine class directly here to make sure all subclasses have the same logger name
                indexWriter.indexService.settings, new ShardId(new Index(indexName), shardId));
        indexWriter.directory = indexWriter.indexService.getLuceneDirectory();
        indexWriter.indexWriter = indexWriter.createWriter();

        return indexWriter;
    }

    public LocalIndexWriter createLocalIndexWriter(String indexName, int shardId, Map<String, String> esConfigs) throws IOException {
        LocalIndexWriter indexWriter = new LocalIndexWriter();
        indexWriter.indexService = new LocalIndexServiceProvider().create(indexName, shardId, esConfigs);
        indexWriter.logger = Loggers.getLogger(Engine.class,
                // we use the engine class directly here to make sure all subclasses have the same logger name
                indexWriter.indexService.settings, new ShardId(new Index(indexName), shardId));
        indexWriter.directory = indexWriter.indexService.getLuceneDirectory();
        indexWriter.indexWriter = indexWriter.createWriter();

        return indexWriter;
    }
}
