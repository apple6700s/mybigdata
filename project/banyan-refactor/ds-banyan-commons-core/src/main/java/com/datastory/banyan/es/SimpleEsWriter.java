package com.datastory.banyan.es;

import com.datastory.banyan.io.DataSinkWriteHook;
import org.apache.commons.collections.CollectionUtils;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;

import java.util.List;
import java.util.Map;

/**
 * com.datastory.banyan.es.SimpleEsWriter
 *
 * @author lhfcws
 * @since 2017/2/10
 */
@Deprecated
public class SimpleEsWriter implements ESWriterAPI {
    protected SimpleEsBulkClient esBulkClient;

    public SimpleEsWriter(SimpleEsBulkClient esBulkClient) {
        this.esBulkClient = esBulkClient;
    }

    public SimpleEsBulkClient getEsBulkClient() {
        return esBulkClient;
    }

    @Override
    public int getCurrentSize() {
        BulkRequest br = null;
        while (true) {
            br = this.esBulkClient.bulkRequest;
            if (br != null) {
                break;
            }
        }
        return br.numberOfActions();
    }

    @Override
    public void update(Map<String, Object> doc) throws Exception {
        if (doc == null || doc.isEmpty())
            return;

        String parent = (String) doc.get("_parent");

        if (parent != null) {
            YZDoc yzDoc = new YZDoc(doc);
            yzDoc.remove("_parent");
            esBulkClient.updateDoc(yzDoc, parent);
        } else {
            YZDoc yzDoc = new YZDoc(doc);
            esBulkClient.updateDoc(yzDoc);
        }
    }

    @Override
    public void write(Map<String, Object> doc) {
        if (doc == null || doc.isEmpty())
            return;

        String parent = (String) doc.get("_parent");

        if (parent != null) {
            YZDoc yzDoc = new YZDoc(doc);
            yzDoc.remove("_parent");
            esBulkClient.addDoc(yzDoc, parent);
        } else {
            YZDoc yzDoc = new YZDoc(doc);
            esBulkClient.addDoc(yzDoc);
        }
    }

    @Override
    public void write(Map<String, Object> doc, IndexRequest.OpType opType) {
        if (doc == null || doc.isEmpty())
            return;

        String parent = (String) doc.get("_parent");
        if (parent != null) {
            YZDoc yzDoc = new YZDoc(doc);
            yzDoc.remove("_parent");
            esBulkClient.addDoc(yzDoc, opType, parent);
        } else {
            YZDoc yzDoc = new YZDoc(doc);
            esBulkClient.addDoc(yzDoc, opType);
        }
    }

    public void write(BulkRequest bulkRequest) {
        if (bulkRequest != null && CollectionUtils.isNotEmpty(bulkRequest.requests())) {
            for (ActionRequest ar : bulkRequest.requests()) {
                this.getEsBulkClient().add(ar);
            }
        }
    }

    @Override
    public void flush() {
        esBulkClient.flush();
    }

    @Override
    public void awaitFlush() {
        esBulkClient.flush(true);
    }

    @Override
    public void close() {
        try {
            esBulkClient.flushNClose();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public List<DataSinkWriteHook> getHooks() {
        return esBulkClient.getHooks();
    }

    public String getIndexName() {
        return esBulkClient.getIndexName();
    }

    public String getIndexType() {
        return esBulkClient.getIndexType();
    }
}
