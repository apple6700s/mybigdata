package com.datastory.commons3.es.lucene_writer;

import com.datastory.commons3.es.lucene_writer.directory.HdfsDirectory;
import org.apache.hadoop.conf.Configuration;
import org.apache.lucene.store.Directory;

import java.io.IOException;

/**
 * com.datastory.commons3.es.lucene_writer.HdfsIndexService
 *
 * @author lhfcws
 * @since 2017/5/4
 */
public class HdfsIndexService extends LocalIndexService {
    protected Configuration conf;

    @Override
    public Directory getLuceneDirectory() throws IOException {
        boolean hdfs = settings.getAsBoolean("lucene.hdfs.enable", false);
        if (hdfs) {
            // if hdfs then ignore following configs
            String path = settings.get("lucene.hdfs.shard." + shardId.getId() + ".path");
            return HdfsDirectory.create(conf, path);
        } else
            return super.getLuceneDirectory();
    }
}
