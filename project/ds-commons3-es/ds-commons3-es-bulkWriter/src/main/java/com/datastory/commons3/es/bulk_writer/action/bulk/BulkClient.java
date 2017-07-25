package com.datastory.commons3.es.bulk_writer.action.bulk;

import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 * com.datastory.commons3.es.bulk_writer.action.bulk.BulkClient
 * A simple es bulk client with forever retry.
 * @author lhfcws
 * @since 2017/5/9
 */
public class BulkClient {
    private static Logger LOG = Logger.getLogger(BulkClient.class);

    protected BulkRequest bulkRequest = new BulkRequest();
    protected TransportClient client;
    protected String index;
    protected String type;
    protected Integer maxBulk;

    private static TransportAddress[] initTransportAddresses(String[] esHosts) {
        TransportAddress[] transportAddresses = new InetSocketTransportAddress[esHosts.length];
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
        return transportAddresses;
    }

    public BulkClient(TransportClient client, String index, String type, int maxBulk) {
        this.client = client;
        this.index = index;
        this.type = type;
        this.maxBulk = maxBulk;
    }

    public BulkClient(Settings settings, String[] esHosts, String index, String type, int maxBulk) {
        this(TransportClient.builder().settings(settings).build().addTransportAddresses(
                initTransportAddresses(esHosts)
                )
                , index, type, maxBulk);
    }

    public BulkClient(String[] esHosts, String cluster, String index, String type, int maxBulk) {
        this(Settings.settingsBuilder()
                        .put("cluster.name", cluster)
                        .put("client.transport.ping_timeout", "60s")
                        .build(),
                esHosts, index, type, maxBulk);
    }

    public int getCurrentNum() {
        return bulkRequest.numberOfActions();
    }

    public String parentField() {
        return "_parent";
    }

    public void add(ActionRequest actionRequest) {
        bulkRequest.add(actionRequest);
        if (getCurrentNum() >= maxBulk)
            flush();
    }

    public void write(Map<String, Object> doc) {
        IndexRequest request = new IndexRequest();

        String id = (String) doc.get("id");
        String _parent = (String) doc.get(parentField());

        if (_parent != null) {
            request.parent(_parent);
        }

        if (id != null) {
            request.id(id);
        }

        request.source(doc);

        add(request);
    }

    public void write(Map<String, Object> doc, IndexRequest.OpType opType) {
        IndexRequest request = new IndexRequest();

        String id = (String) doc.get("id");
        String _parent = (String) doc.get(parentField());

        if (_parent != null) {
            request.parent(_parent);
        }

        if (id != null) {
            request.id(id);
        }

        if (opType != null)
            request.opType(opType);

        request.source(doc);

        add(request);
    }

    public void update(Map<String, Object> doc) throws Exception {
        UpdateRequest request = new UpdateRequest();

        String id = (String) doc.get("id");
        if (id == null)
            throw new IOException("id should not be null in Update");
        request.id(id);

        String _parent = (String) doc.get(parentField());
        if (_parent != null) {
            request.parent(_parent);
        }

        XContentBuilder builder = XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE);
        builder.map(doc);
        request.source(builder);
    }

    public void flush() {
        if (getCurrentNum() > 0) {
            synchronized (maxBulk) {
                if (getCurrentNum() > 0) {
                    BulkRequest toFlush = bulkRequest;
                    bulkRequest = new BulkRequest();
                    client.bulk(toFlush, getBulkResponseListener());
                }
            }
        }
    }

    public void syncFlush() {
        if (getCurrentNum() > 0) {
            synchronized (maxBulk) {
                if (getCurrentNum() > 0) {
                    BulkRequest toFlush = bulkRequest;
                    bulkRequest = new BulkRequest();
                    try {
                        ActionFuture<BulkResponse> future = client.bulk(toFlush);
                        BulkResponse bulkResponse = future.actionGet();
                        getBulkResponseListener().onResponse(bulkResponse);
                    } catch (Throwable e) {
                        getBulkResponseListener().onFailure(e);
                    }
                }
            }
        }
    }

    public void flusnNClose() {
        while (getCurrentNum() > 0) {
            syncFlush();
        }
        close();
    }

    public void close() {
        client.close();
    }

    public ActionListener<BulkResponse> getBulkResponseListener() {
        return new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkResponse) {
                int i = 0;
                for (BulkItemResponse bulkItemResponse : bulkResponse) {
                    if (bulkItemResponse.isFailed()) {
                        LOG.error(bulkItemResponse.getFailureMessage(), bulkItemResponse.getFailure().getCause());
                        if (BulkItemResponseState.isBulkResponseSuccess(bulkItemResponse)) {
                            add(bulkRequest.requests().get(i));
                        }
                    }
                    i++;
                }
            }

            @Override
            public void onFailure(Throwable e) {
                LOG.error("[Bulk failed] " + e.getMessage(), e);
            }
        };
    }
}
