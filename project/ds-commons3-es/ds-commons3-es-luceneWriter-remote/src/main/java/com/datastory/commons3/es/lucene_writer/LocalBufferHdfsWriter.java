package com.datastory.commons3.es.lucene_writer;

import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.index.IndexRequest;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * com.datastory.commons3.es.lucene_writer.LocalBufferHdfsWriter
 *
 * @author lhfcws
 * @since 2017/6/12
 */
public class LocalBufferHdfsWriter {
    public static final int DFT_MAX_BUFFER_SIZE = 100000;
    public static final int MAX_PER_MERGE_TIMES = 10;

    String localBufferPath;
    String remoteHdfsPath;
    LocalIndexWriter bufferWriter;
    HdfsIndexWriter mergeWriter;

    int maxBufferSize;
    AtomicInteger bufferSize = new AtomicInteger(0);
    AtomicInteger mergeTimes = new AtomicInteger(0);


    private LocalBufferHdfsWriter() {

    }

    public LocalIndexWriter getBufferWriter() {
        return bufferWriter;
    }

    public HdfsIndexWriter getMergeWriter() {
        return mergeWriter;
    }

    public int getMaxBufferSize() {
        return maxBufferSize;
    }

    public String getLocalBufferPath() {
        return localBufferPath;
    }

    public String getRemoteHdfsPath() {
        return remoteHdfsPath;
    }

    public int getCurrentBufferSize() {
        return bufferSize.get();
    }

    public synchronized void write(List<IndexRequest> indexRequests) throws IOException {
        int currentSize = bufferSize.addAndGet(indexRequests.size());
        this.bufferWriter.write(indexRequests);
        if (currentSize > maxBufferSize) {
            this.flush();
        }
    }

    public void flush() throws IOException {
        Directory localDir = this.bufferWriter.getDirectory();
        this.bufferWriter.flush();
        this.bufferWriter.close();
        mergeTimes.incrementAndGet();

        try {
            int retry = 3;
            Exception exception = null;
            while (retry-- > 0) {
                try {
                    long start = System.currentTimeMillis();
                    mergeWriter.addIndex(localDir);
                    mergeWriter.flush();

                    long diff = System.currentTimeMillis() - start;
                    this.mergeWriter.logger.info("Merge from local , took [" + diff + "ms]");
                    System.out.println("Merge from local , took [" + diff + "ms]");
                    bufferSize.set(0);
                    exception = null;

                    break;
                } catch (Exception e) {
                    exception = e;
                    e.printStackTrace();
                }
            }

            if (mergeTimes.get() > MAX_PER_MERGE_TIMES) {
                this.mergeWriter.close();
                this.mergeWriter.indexWriter = this.mergeWriter.createWriter();
                mergeTimes.set(0);
            }

            if (exception != null) {
                throw new IOException(exception);
            }
        } finally {
            for (String fn : localDir.listAll()) {
                localDir.deleteFile(fn);
            }
            this.bufferWriter.indexWriter = this.bufferWriter.createWriter();
        }
    }

    public void close() throws IOException {
        this.bufferWriter.close();
        this.mergeWriter.close();
    }

    /**********************************
     * Builder of LocalBufferHdfsWriter
     */
    public static class Builder {
        Configuration conf;
        String localBufferPath;
        String remoteHdfsPath;
        String indexName;
        int shardId;
        int maxBufferSize = DFT_MAX_BUFFER_SIZE;

        public Builder(Configuration conf) {
            this.conf = conf;
        }

        public Builder setLocalBufferPath(String localBufferPath) {
            this.localBufferPath = localBufferPath;
            return this;
        }

        public Builder setRemoteHdfsPath(String remoteHdfsPath) {
            this.remoteHdfsPath = remoteHdfsPath;
            return this;
        }

        public Builder setIndexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public Builder setShardId(int shardId) {
            this.shardId = shardId;
            return this;
        }

        public Builder setMaxBufferSize(int maxBufferSize) {
            this.maxBufferSize = maxBufferSize;
            return this;
        }

        public LocalBufferHdfsWriter build() throws IOException {
            return build(null);
        }

        public LocalBufferHdfsWriter build(Map<String, String> esConfigs) throws IOException {
            DefaultSettings.initEmptyPathHome();

            if (esConfigs == null)
                esConfigs = new HashMap<>();

            LocalBufferHdfsWriter localBufferHdfsWriter = new LocalBufferHdfsWriter();
            localBufferHdfsWriter.localBufferPath = localBufferPath;
            localBufferHdfsWriter.remoteHdfsPath = remoteHdfsPath;
            localBufferHdfsWriter.maxBufferSize = maxBufferSize;

            if (localBufferPath != null)
                esConfigs.put("local.path.data", localBufferPath);

            localBufferHdfsWriter.bufferWriter = new LocalIndexWriterFactory().createLocalIndexWriter(
                    indexName, shardId, esConfigs
            );

            esConfigs.put("lucene.hdfs.enable", "true");
            if (remoteHdfsPath != null)
                esConfigs.put("lucene.hdfs.shard." + shardId + ".path", remoteHdfsPath);
            localBufferHdfsWriter.mergeWriter = new HdfsIndexWriterFactory().createHdfsIndexWriter(
                    indexName, shardId, esConfigs, conf
            );

            return localBufferHdfsWriter;
        }
    }
}
