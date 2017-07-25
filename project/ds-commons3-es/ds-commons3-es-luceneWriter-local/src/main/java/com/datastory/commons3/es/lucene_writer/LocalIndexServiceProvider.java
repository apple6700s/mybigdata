package com.datastory.commons3.es.lucene_writer;

import java.io.IOException;
import java.util.Map;

/**
 * com.datastory.commons3.es.lucene_writer.LocalIndexServiceFactory
 *
 * @author lhfcws
 * @since 2017/6/6
 */
public class LocalIndexServiceProvider {
    public LocalIndexService create(String indexName, int sharIdNum) throws IOException {
        return create(indexName, sharIdNum, null);
    }

    public LocalIndexService create(String indexName, int sharIdNum, Map<String, String> configs) throws IOException {
        LocalIndexService localIndexService = new LocalIndexService();
        localIndexService.init(indexName, sharIdNum, configs);
        return localIndexService;
    }
}
