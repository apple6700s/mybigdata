package com.datastory.banyan.es;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.io.DataSinkWriter;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.DateUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;
import org.apache.spark.annotation.Experimental;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.transport.TransportClient;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * com.datastory.banyan.es.ESWriter
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class ESWriter implements DataSinkWriter, ESWriterAPI {
    protected static Logger LOG = Logger.getLogger(ESWriter.class);

    public static final long MAX_IDLE_TIME = 30 * 60 * 1000;    // 30 min
    public static final int DFT_MAX_BULK_NUM = 2000;

    protected SimpleEsBulkClient esBulkClient;
    protected String clusterName;
    protected String indexName;
    protected String indexType;
    protected String[] esHosts;
    protected int bulkNum = DFT_MAX_BULK_NUM;

    protected static RhinoETLConfig conf = RhinoETLConfig.getInstance();
    protected volatile long lastUpdateTime = System.currentTimeMillis();
    protected final AtomicBoolean alive = new AtomicBoolean(true);
    protected boolean enableIdleClose = false;

    public ESWriter(String indexName, String indexType) {
        this(indexName, indexType, DFT_MAX_BULK_NUM);
    }

    public ESWriter(String indexName, String indexType, int bulkNum) {
        this(conf.getStrings(RhinoETLConsts.ES_HOSTS_BULK), indexName, indexType, bulkNum);
    }

    public ESWriter(String esHost, String indexName, String indexType) {
        this(new String[]{esHost}, indexName, indexType, DFT_MAX_BULK_NUM);
    }

    public ESWriter(String esHost, String indexName, String indexType, int bulkNum) {
        this(new String[]{esHost}, indexName, indexType, bulkNum);
    }

    public ESWriter(String[] esHosts, String indexName, String indexType, int bulkNum) {
        System.out.println("ESWriter : " + indexName + "." + indexType + " es.hosts=" + Arrays.toString(esHosts) + " , bulk size = " + bulkNum);
        this.clusterName = conf.get(RhinoETLConsts.ES_CLUSTER_NAME);
        this.indexName = indexName;
        this.indexType = indexType;
        this.bulkNum = bulkNum;
        // 打乱esHost，因为默认esclient是递增获取node，多进程同时写，会造成单点瓶颈
        this.esHosts = BanyanTypeUtil.shuffleArray(esHosts);

        initBulkClient();
    }

    @Experimental
    public boolean isEnableIdleClose() {
        return enableIdleClose;
    }

    @Experimental
    public void setEnableIdleClose(boolean enableIdleClose) {
        this.enableIdleClose = enableIdleClose;
    }

    public boolean isAlive() {
        return alive.get();
    }

    public void initBulkClient() {
        if (esBulkClient != null)
            try {
                esBulkClient.close();
                esBulkClient = null;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        esBulkClient = ESClientFactory.createBanyanEsBulkClient(esHosts, clusterName, indexName, indexType, bulkNum);

        alive.set(true);
        LOG.info(String.format("Reinitiallized EsWriter[%s] at %s", this.getClass().getSimpleName() + "-" + esBulkClient.getClass().getSimpleName(), DateUtils.getCurrentPrettyTimeStr()));
        LOG.error(String.format("[INFO] Reinitiallized EsWriter[%s] at %s", this.getClass().getSimpleName() + "-" + esBulkClient.getClass().getSimpleName(), DateUtils.getCurrentPrettyTimeStr()));
    }

    public SimpleEsBulkClient getEsBulkClient() {
        return esBulkClient;
    }

    public void updateLastUpdateTime() {
        lastUpdateTime = System.currentTimeMillis();
    }

    @Override
    public int getCurrentSize() {
        return esBulkClient.getCurrentSize();
    }

    public int getBulkNum() {
        return bulkNum;
    }

    public void update(Map<String, Object> doc) throws Exception {
        if (doc == null || doc.isEmpty())
            return;
        if (enableIdleClose) {
            synchronized (alive) {
                updateLastUpdateTime();
            }
            if (!isAlive()) {
                synchronized (alive) {
                    if (!isAlive()) {
                        initBulkClient();
                    }
                }
            }
        }

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

    public void write(Map<String, Object> doc) {
        if (doc == null || doc.isEmpty())
            return;
        if (enableIdleClose) {
            synchronized (alive) {
                updateLastUpdateTime();
            }
            if (!isAlive()) {
                synchronized (alive) {
                    if (!isAlive()) {
                        initBulkClient();
                    }
                }
            }
        }

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

    public void write(Map<String, Object> doc, IndexRequest.OpType opType) {
        if (doc == null || doc.isEmpty())
            return;
        if (enableIdleClose) {
            synchronized (alive) {
                updateLastUpdateTime();
            }
            if (!isAlive()) {
                synchronized (alive) {
                    if (!isAlive()) {
                        initBulkClient();
                    }
                }
            }
        }

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
        if (bulkRequest == null || bulkRequest.requests().isEmpty())
            return;

        if (enableIdleClose) {
            synchronized (alive) {
                updateLastUpdateTime();
            }
            if (!isAlive()) {
                synchronized (alive) {
                    if (!isAlive()) {
                        initBulkClient();
                    }
                }
            }
        }

        if (CollectionUtils.isNotEmpty(bulkRequest.requests())) {
            for (ActionRequest ar : bulkRequest.requests()) {
                this.getEsBulkClient().add(ar);
            }
        }
    }

    public int getCurrentAvailableNodes() {
        TransportClient transportClient = esBulkClient.client;
        if (transportClient != null) {
            try {
                return transportClient.connectedNodes().size();
            } catch (NullPointerException ignore) {
                return 0;
            }
        } else
            return 0;
    }

    @Experimental
    public void closeIfIdle() {
        if (!enableIdleClose) {
            return;
        }

        if (getCurrentSize() > 0) {
            flush();
            return;
        }

        // if enableIdleClose && getCurrentSize() == 0
        boolean needClose = false;
        if (isAlive())
            synchronized (alive) {
                if (isAlive()) {
                    int connectedNodesNum = getCurrentAvailableNodes();
                    if (connectedNodesNum < esHosts.length / 2) {
                        LOG.error("Too many nodes failed, current nodes num : " + connectedNodesNum + ", expected total: " + esHosts.length);
                        LOG.info("Too many nodes failed, current nodes num : " + connectedNodesNum + ", expected total: " + esHosts.length);
                        needClose = true;
                    }
                }
            }

        long now = System.currentTimeMillis();
        if (isAlive() && (needClose || now - lastUpdateTime > MAX_IDLE_TIME)) {
            try {
                synchronized (alive) {
                    if (isAlive() && (needClose || now - lastUpdateTime > MAX_IDLE_TIME)) {
                        alive.set(false);
                        close();
                        if (needClose)
                            LOG.error(String.format("[INFO] Closed EsWriter[%s] at %s", this.getClass().getSimpleName(), DateUtils.getCurrentPrettyTimeStr()));
                        else
                            LOG.error(String.format("[INFO] Closed idle EsWriter[%s] at %s", this.getClass().getSimpleName(), DateUtils.getCurrentPrettyTimeStr()));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public String getIndexName() {
        return esBulkClient.getIndexName();
    }

    public String getIndexType() {
        return esBulkClient.getIndexType();
    }

    public List<DataSinkWriteHook> getHooks() {
        return esBulkClient.getHooks();
    }

    public void flush() {
        esBulkClient.flush();
    }

    public void awaitFlush() {
        esBulkClient.flush(true);
    }

    public ESWriter setSyncMode() {
        this.getEsBulkClient().setSyncMode();
        return this;
    }

    public void close() {
        try {
            if (esBulkClient != null) {
                esBulkClient.flushNClose();
                esBulkClient = null;
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
