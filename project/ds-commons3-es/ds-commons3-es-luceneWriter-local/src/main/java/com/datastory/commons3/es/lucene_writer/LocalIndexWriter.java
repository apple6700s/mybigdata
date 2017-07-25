package com.datastory.commons3.es.lucene_writer;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.LoggerInfoStream;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.mapper.ParseContext.Document;
import org.elasticsearch.index.shard.ElasticsearchMergePolicy;
import org.elasticsearch.index.shard.MergePolicyConfig;
import org.elasticsearch.index.shard.MergeSchedulerConfig;
import org.elasticsearch.indices.memory.IndexingMemoryController;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * com.datastory.commons3.es.lucene_writer.LocalIndexWriter
 * 较重，建议不要频繁初始化
 *
 * @author lhfcws
 * @since 2017/4/28
 */
public class LocalIndexWriter {
    ESLogger logger;
    // data directory
    Directory directory;
    IndexWriter indexWriter;
    LocalIndexService indexService;

    LocalIndexWriter() {
    }

    public Directory getDirectory() {
        return directory;
    }

    public void setData(Map<String, String> data) {
        indexWriter.setCommitData(data);
    }

    public void write(IndexRequest indexRequest) throws IOException {
        Engine.Index index = indexService.toIndex(indexRequest);
        write(index);
    }

    public void write(List<IndexRequest> indexRequests) throws IOException {
        List<Document> documents = new LinkedList<>();
        for (IndexRequest indexRequest : indexRequests) {
            Engine.Index index = indexService.toIndex(indexRequest);
            documents.addAll(index.docs());
        }

        if (documents.size() > 1) {
            indexWriter.addDocuments(documents);
        } else if (documents.size() == 1) {
            indexWriter.addDocument(documents.get(0));
        }
    }

    public synchronized void write(Engine.Index index) throws IOException {
        if (index.docs().size() > 1) {
            indexWriter.addDocuments(index.docs());
        } else if (index.docs().size() == 1) {
            indexWriter.addDocument(index.docs().get(0));
        }
    }

    public synchronized void addIndex(Directory ... otherIndexes) throws IOException {
        indexWriter.addIndexes(otherIndexes);
    }


    public synchronized void flush() throws IOException {
        indexWriter.flush();
    }

    public synchronized void close() throws IOException {
        indexWriter.close();
    }

    public void forceMerge(int maxMergedSegments) throws IOException {
        indexWriter.forceMerge(maxMergedSegments);
    }

    public boolean existsDirectory() throws IOException {
        return Lucene.indexExists(directory);
    }

    /**
     * default openmode is create mode
     *
     * @return
     * @throws IOException
     */
    public IndexWriter createWriter() throws IOException {
        return createWriter(!existsDirectory());
    }

    public IndexWriter createWriter(boolean create) throws IOException {
        try {
            final IndexWriterConfig iwc = new IndexWriterConfig(
                    indexService.getMapperService().indexAnalyzer()
            );
            iwc.setCommitOnClose(true); // we by default don't commit on close
            iwc.setOpenMode(create ? IndexWriterConfig.OpenMode.CREATE : IndexWriterConfig.OpenMode.APPEND);
            iwc.setIndexDeletionPolicy(indexService.indexDeletionPolicy);
            // with tests.verbose, lucene sets this up: plumb to align with filesystem stream
            boolean verbose = true;
            iwc.setInfoStream(verbose ? InfoStream.getDefault() : new LoggerInfoStream(logger));
            MergeSchedulerConfig mergeSchedulerConfig = new MergeSchedulerConfig(indexService.settings);
            MergeScheduler mergeScheduler = new ElasticsearchConcurrentMergeScheduler(indexService.shardId, indexService.settings,
                    mergeSchedulerConfig);
            iwc.setMergeScheduler(mergeScheduler);
            MergePolicy mergePolicy = new MergePolicyConfig(logger, indexService.settings).getMergePolicy();
            // Give us the opportunity to upgrade old segments while performing
            // background merges
            mergePolicy = new ElasticsearchMergePolicy(mergePolicy);
            iwc.setMergePolicy(mergePolicy);
            iwc.setSimilarity(indexService.similarityService.similarity());
            ByteSizeValue indexingBufferSize = IndexingMemoryController.INACTIVE_SHARD_INDEXING_BUFFER;
            iwc.setRAMBufferSizeMB(indexingBufferSize.mbFrac());
            iwc.setCodec(indexService.getCodec());
            /* We set this timeout to a highish value to work around
             * the default poll interval in the Lucene lock that is
             * 1000ms by default. We might need to poll multiple times
             * here but with 1s poll this is only executed twice at most
             * in combination with the default writelock timeout*/
            iwc.setWriteLockTimeout(5000);
            iwc.setUseCompoundFile(indexService.settings.getAsBoolean(EngineConfig.INDEX_COMPOUND_ON_FLUSH, true));
            IndexWriter indexWriter = new IndexWriter(directory, iwc);

            return indexWriter;
        } catch (Exception ex) {
            throw ex;
        }
    }
}
