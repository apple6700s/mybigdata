package com.datastory.banyan.es;

import org.elasticsearch.action.index.IndexRequest;

import java.io.Serializable;
import java.util.Map;

/**
 * com.datastory.banyan.es.ESWriterAPI
 *
 * @author lhfcws
 * @since 2016/12/20
 */
public interface ESWriterAPI extends Serializable {
    public int getCurrentSize();
    public void update(Map<String, Object> doc) throws Exception;
    public void write(Map<String, Object> doc);
    public void write(Map<String, Object> doc, IndexRequest.OpType opType);
    public void flush();
    public void awaitFlush();
    public void close();
}
