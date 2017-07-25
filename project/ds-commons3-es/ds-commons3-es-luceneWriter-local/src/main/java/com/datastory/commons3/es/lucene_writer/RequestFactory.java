package com.datastory.commons3.es.lucene_writer;

import org.elasticsearch.action.index.IndexRequest;

import java.util.Map;

/**
 * com.datastory.commons3.es.lucene_writer.RequestFactory
 *
 * @author lhfcws
 * @since 2017/5/2
 */
public class RequestFactory {
    public static IndexRequest indexRequest(String index, String type, Map<String, Object> map) {
        IndexRequest indexRequest = new IndexRequest(index, type);
        indexRequest.id((String) map.get("id"));
        indexRequest.source(FastJsonSerializer.serialize(map));
        indexRequest.timestamp(System.currentTimeMillis() + "");

        if (map.containsKey("_parent")) {
            String parent = (String) map.remove("_parent");
            indexRequest.parent(parent);
        }

        return indexRequest;
    }
}
