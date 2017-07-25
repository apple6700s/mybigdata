package com.datastory.commons3.es.lucene_writer;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Map;

/**
 * com.datastory.commons3.es.lucene_writer.HdfsIndexServiceProvider
 *
 * @author lhfcws
 * @since 2017/6/6
 */
public class HdfsIndexServiceProvider {
    public HdfsIndexService create(String indexName, int sharIdNum, Map<String, String> configs, Configuration conf) throws IOException {
        HdfsIndexService hdfsIndexService = new HdfsIndexService();
        hdfsIndexService.conf = new Configuration(conf);
        hdfsIndexService.init(indexName, sharIdNum, configs);
        return hdfsIndexService;
    }
}
