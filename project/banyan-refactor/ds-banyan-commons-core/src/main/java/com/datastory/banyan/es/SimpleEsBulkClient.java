package com.datastory.banyan.es;

import com.datastory.banyan.es.hooks.ESRetryLogHook;
import com.datastory.banyan.es.hooks.ESWriteHook;
import com.datastory.banyan.es.hooks.EsMonitorHook;
import com.datastory.banyan.es.override233.DsIndexRequest;
import com.datastory.banyan.es.override233.DsUpdateRequest;
import com.datastory.banyan.io.DataSinkWriteHook;
import com.datastory.banyan.io.DataSinkWriter;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.Condition;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * com.datastory.banyan.es.SimpleEsBulkClient
 *
 * @author lhfcws
 * @since 2017/2/10
 */
public class SimpleEsBulkClient implements DataSinkWriter {
    protected String indexName = null;
    protected String indexType = null;
    protected int bulkActions;
    protected AtomicBoolean flushing = new AtomicBoolean(false);
    protected AtomicInteger syncMode = new AtomicInteger(0); // 0 for async, 1 for sync
    protected AtomicInteger status = new AtomicInteger(0); // 0 for running, -1 for stopped

    protected TransportClient client;
    protected volatile BulkRequest bulkRequest;
    protected List<DataSinkWriteHook> dataSinkWriteHooks = new LinkedList<>();

    public SimpleEsBulkClient(String clusterName, String indexName, String indexType,
                              String[] hosts, int bulkActions) {
        this(Settings.settingsBuilder()
                        .put("cluster.name", clusterName)
                        .put("client.transport.ping_timeout", "60s")
                        .build(),
                indexName, indexType, hosts, bulkActions);
    }

    protected SimpleEsBulkClient(Settings settings, String indexName, String indexType,
                                 String[] esHosts, int bulkActions) {
        this.indexName = indexName;
        this.indexType = indexType;
        this.bulkActions = bulkActions;
        TransportAddress[] transportAddresses = new InetSocketTransportAddress[esHosts.length];
        esHosts = BanyanTypeUtil.shuffleCopyArray(esHosts);
        for (int i = 0; i < esHosts.length; i++) {
            String[] parts = esHosts[i].split(":");
            int port = 9300;
            if (parts.length > 1)
                port = Integer.parseInt(parts[1]);
            try {
                InetAddress inetAddress = InetAddress.getByName(parts[0]);
                transportAddresses[i] = new InetSocketTransportAddress(inetAddress, port);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        client = TransportClient.builder().settings(settings).build().addTransportAddresses(transportAddresses);
        bulkRequest = new BulkRequest();

//        this.getHooks().add(getMainHook());
    }

    @Override
    public List<DataSinkWriteHook> getHooks() {
        return dataSinkWriteHooks;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getIndexType() {
        return indexType;
    }

    public String getBulkName() {
        return indexName + "." + indexType;
    }

    public Client getClient() {
        return client;
    }

    public void setSyncMode() {
        this.syncMode.compareAndSet(0, 1);
    }

    public boolean isSyncMode() {
        return this.syncMode.get() == 1;
    }

    public void setAsyncMode() {
        this.syncMode.compareAndSet(1, 0);
    }

    public void addDoc(YZDoc doc) {
        addDoc(doc, "");
    }

    public void addDoc(YZDoc doc, String parent) {
        if (doc == null || doc.getId() == null)
            return;

        IndexRequest indexRequest = Requests.indexRequest(indexName).type(indexType);

        String id = doc.getId();
        if (null != id) {
            indexRequest.id(id);
        }

        if (!StringUtils.isEmpty(parent)) {
            indexRequest.parent(parent);
        }

        indexRequest.source(doc.toJson());
        add(indexRequest);
    }

    public boolean deleteDoc(String id) {
        this.deleteDoc(id, null);
        return true;
    }

    public boolean deleteDoc(String id, String parent) {
        if (StringUtils.isEmpty(id))
            return false;

        DeleteRequest deleteRequest = Requests.deleteRequest(indexName).type(indexType).id(id);

        if (!StringUtils.isEmpty(parent)) {
            deleteRequest.parent(parent);
        }

        add(deleteRequest);
        return true;
    }

    public void updateDoc(YZDoc doc) throws Exception {
        this.updateDoc(doc, null);
    }

    public void updateDoc(YZDoc doc, String parent) throws Exception {
        if (doc == null || doc.getId() == null)
            return;

        String id = doc.getId();

        UpdateRequest updateRequest = new DsUpdateRequest(indexName, indexType, id);

        if (!StringUtils.isEmpty(parent)) {
            updateRequest.parent(parent);
        }
        updateRequest.source(doc.toJson().getBytes());
        add(updateRequest);
    }

    public void addDoc(YZDoc doc, IndexRequest.OpType opType) {
        addDoc(doc, opType, null);
    }

    public void addDoc(YZDoc doc, IndexRequest.OpType opType, String parent) {
        if (doc == null || doc.getId() == null)
            return;
        IndexRequest indexRequest = new DsIndexRequest(indexName, indexType);

        indexRequest.opType(opType);
        String id = doc.getId();
        if (null != id) {
            indexRequest.id(id);
        }

        if (!StringUtils.isEmpty(parent)) {
            indexRequest.parent(parent);
        }
        indexRequest.source(doc.toJson());

        add(indexRequest);
    }

    public void add(ActionRequest ar) {
        if (isSyncMode()) {
            try {
                new Condition() {
                    @Override
                    public boolean satisfy() {
                        return ! (isSyncMode() && flushing.get());
                    }
                }.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        bulkRequest.add(ar);
        if (bulkRequest.numberOfActions() > bulkActions) {
            flush();
        }
    }

    public void close() throws InterruptedException {
        status.compareAndSet(0, -1);
        client.close();
    }

    public void flushNClose() throws InterruptedException {
        status.compareAndSet(0, -1);
        flush(true);
        close();
    }

    public void flush() {
        flush(false);
    }

    public void flush(final boolean await) {
        if (!bulkRequest.requests().isEmpty()) {
            if (isSyncMode()) {
                flushing.compareAndSet(false, true);
            }

            final ESWriteHook esWriteHook;
            final BulkRequest br;
            synchronized (this) {
                if (!bulkRequest.requests().isEmpty()) {
                    esWriteHook = (ESWriteHook) getMainHook();
                    br = bulkRequest;
                    bulkRequest = new BulkRequest();
                } else
                    return;
            }

            esWriteHook.beforeWrite(br);
            final CountDownLatch latch = new CountDownLatch(1);
            client.bulk(br, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    esWriteHook.afterWrite(br, bulkItemResponses);
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable e) {
                    esWriteHook.afterWrite(br, e);
                    latch.countDown();
                }
            });

            if (await && !getHooks().isEmpty())
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            if (isSyncMode()) {
                flushing.compareAndSet(true, false);
            }
        }
    }

    public int getCurrentSize() {
        synchronized (this) {
            return bulkRequest.numberOfActions();
        }
    }

    public DataSinkWriteHook getMainHook() {
        final DataSinkWriteHook[] localHooks = new DataSinkWriteHook[]{
                new EsMonitorHook(getBulkName()),
                new ESRetryLogHook(getBulkName())
        };
        return new ESWriteHook(this, this.indexName + "." + this.indexType, false) {
            @Override
            public void beforeWrite(Object writeRequest) {
                for (DataSinkWriteHook dataSinkWriteHook : localHooks) {
                    dataSinkWriteHook.beforeWrite(writeRequest);
                }
            }

            @Override
            public void afterWrite(Object writeRequest, Object writeResponse) {
                for (DataSinkWriteHook dataSinkWriteHook : localHooks) {
                    dataSinkWriteHook.afterWrite(writeRequest, writeResponse);
                }
            }
        };
    }
}
