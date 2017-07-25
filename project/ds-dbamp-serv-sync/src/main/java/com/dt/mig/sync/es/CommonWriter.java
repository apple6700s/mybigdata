package com.dt.mig.sync.es;

/**
 * Created by abel.chan on 16/12/22.
 */

import com.ds.dbamp.core.base.cfg.DbampConfiguration;
import com.ds.dbamp.core.base.consts.es.DbampEsConsts;
import com.ds.dbamp.core.dao.es.YZDoc;
import com.ds.dbamp.core.dao.es.writer.BulkYZIndexWriter;
import com.dt.mig.sync.utils.BanyanTypeUtil;
import org.apache.hadoop.conf.Configuration;
import org.elasticsearch.action.index.IndexRequest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;


public class CommonWriter {

    private BulkYZIndexWriter bulkYZIndexWriter;

    private static final int MAX_BULK_NUM = 2000;

    public CommonWriter(String indexName, String indexType) {
        Configuration conf = DbampConfiguration.getInstance();
        String[] esHosts = conf.getStrings(DbampEsConsts.ES_HOSTS);
        esHosts = BanyanTypeUtil.shuffleCopyArray(esHosts);
        String clusterName = conf.get(DbampEsConsts.ES_CLUSTER_NAME);
        System.out.println(indexName + "." + indexType + ", es hosts: " + Arrays.asList(esHosts));
        bulkYZIndexWriter = new BulkYZIndexWriter(clusterName, indexName, indexType, esHosts, MAX_BULK_NUM);
    }

    public CommonWriter(String indexName, String indexType, int bulkNum) {
        Configuration conf = DbampConfiguration.getInstance();
        String[] esHosts = conf.getStrings(DbampEsConsts.ES_HOSTS);
        esHosts = BanyanTypeUtil.shuffleCopyArray(esHosts);
        String clusterName = conf.get(DbampEsConsts.ES_CLUSTER_NAME);
        System.out.println(indexName + "." + indexType + ", es hosts: " + Arrays.asList(esHosts));
        bulkYZIndexWriter = new BulkYZIndexWriter(clusterName, indexName, indexType, esHosts, bulkNum);
    }

    public CommonWriter(Configuration conf, String clusterName, String indexName, String indexType) throws Exception {
        String[] esHosts = conf.getStrings(DbampEsConsts.ES_HOSTS);
        esHosts = BanyanTypeUtil.shuffleCopyArray(esHosts);
        System.out.println("es hosts: " + Arrays.asList(esHosts));
        bulkYZIndexWriter = new BulkYZIndexWriter(clusterName, indexName, indexType, esHosts, MAX_BULK_NUM);
    }

    public CommonWriter(Configuration conf, String clusterName, String indexName, String indexType, int bulkNum) throws Exception {
        String[] esHosts = conf.getStrings(DbampEsConsts.ES_HOSTS);
        esHosts = BanyanTypeUtil.shuffleCopyArray(esHosts);
        System.out.println("es hosts: " + Arrays.asList(esHosts));
        bulkYZIndexWriter = new BulkYZIndexWriter(clusterName, indexName, indexType, esHosts, bulkNum);
    }

    public void write(Map<String, Object> doc) throws IOException {
        if (doc == null || doc.isEmpty()) return;

        String parent = (String) doc.get("_parent");

        if (parent != null) {
            YZDoc yzDoc = new YZDoc(doc);
            yzDoc.remove("_parent");
            addData(yzDoc, parent);
        } else {
            YZDoc yzDoc = new YZDoc(doc);
            addData(yzDoc);
        }
    }

    public void write(YZDoc doc) throws IOException {
        String parent = (String) doc.get("_parent");

        if (parent != null) {
            YZDoc yzDoc = new YZDoc(doc);
            yzDoc.remove("_parent");
            addData(yzDoc, parent);
        } else {
            YZDoc yzDoc = new YZDoc(doc);
            addData(yzDoc);
        }
    }


    public void addData(YZDoc yzDoc) throws IOException {
        if (yzDoc != null && yzDoc.getId() != null) {
            bulkYZIndexWriter.addDoc(yzDoc);
        }
    }

    public void addData(YZDoc yzDoc, String parent) throws IOException {
        if (yzDoc != null && yzDoc.getId() != null) {
            bulkYZIndexWriter.addDoc(yzDoc, parent);
        }
    }

    public void addData(YZDoc yzDoc, IndexRequest.OpType opType) throws IOException {
        if (yzDoc != null && yzDoc.getId() != null) {
            bulkYZIndexWriter.addDoc(yzDoc, opType);
        }
    }

    public void addData(YZDoc yzDoc, IndexRequest.OpType opType, String parent) throws IOException {
        if (yzDoc != null && yzDoc.getId() != null) {
            bulkYZIndexWriter.addDoc(yzDoc, opType, parent);
        }
    }

    public void updateData(YZDoc yzDoc) throws IOException {
        if (yzDoc != null && yzDoc.getId() != null) {
            bulkYZIndexWriter.updateDoc(yzDoc);
        }
    }

    public void updateData(YZDoc yzDoc, String parent) throws IOException {
        if (yzDoc != null && yzDoc.getId() != null) {
            bulkYZIndexWriter.updateDoc(yzDoc, parent);
        }
    }

    public void updateDataWithMap(YZDoc yzDoc, String parent) throws IOException {
        if (yzDoc != null && yzDoc.getId() != null) {
            bulkYZIndexWriter.updateDocWithMap(yzDoc, parent);
        }
    }

    public void deleteData(YZDoc yzDoc) {
        if (yzDoc != null && yzDoc.getId() != null) {
            bulkYZIndexWriter.deleteDoc(yzDoc.getId());
        }
    }

    public void deleteData(YZDoc yzDoc, String parent) {
        if (yzDoc != null && yzDoc.getId() != null) {
            bulkYZIndexWriter.deleteDoc(yzDoc.getId(), parent);
        }
    }

    public synchronized void flush() {
        bulkYZIndexWriter.flush();
    }

    public synchronized void close() {
        bulkYZIndexWriter.close();
    }
}

