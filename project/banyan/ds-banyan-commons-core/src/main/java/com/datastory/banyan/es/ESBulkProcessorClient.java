package com.datastory.banyan.es;

import com.datastory.banyan.es.override233.DsBulkProcessor;
import com.datastory.banyan.utils.ShutdownHookManger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.util.concurrent.TimeUnit;


/**
 * com.datastory.banyan.es.ESBulkProcessorClient
 */
public class ESBulkProcessorClient extends SimpleEsBulkClient {
    protected TimeValue flushInterval = null;

    protected DsBulkProcessor bulkProcessor;

    public ESBulkProcessorClient(String clusterName, String indexName, String indexType,
                                 String[] hosts, int bulkActions) {
        this(Settings.settingsBuilder()
                        .put("cluster.name", clusterName)
                        .put("client.transport.ping_timeout", "60s")
                        .build(),
                indexName, indexType, hosts, bulkActions, null);
    }

    public ESBulkProcessorClient(String clusterName, String indexName, String indexType,
                                 String[] hosts, int bulkActions, TimeValue flushInterval) {
        this(Settings.settingsBuilder()
                        .put("cluster.name", clusterName)
                        .put("client.transport.ping_timeout", "60s")
                        .build(),
                indexName, indexType, hosts, bulkActions, flushInterval);
    }

    protected ESBulkProcessorClient(Settings settings, String indexName, String indexType,
                                    String[] esHosts, int bulkActions, TimeValue flushInterval) {
        super(settings, indexName, indexType, esHosts, bulkActions);
        this.flushInterval = flushInterval;
        bulkProcessor = createBulkProcessor(client, indexName + "." + indexType);

        ShutdownHookManger.addShutdownHook("[" + this.getClass().getSimpleName() + "] " + this.getBulkName(), new Runnable() {
            @Override
            public void run() {
                flush();
                try {
                    close();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        this.getHooks().clear();
    }

    public DsBulkProcessor getBulkProcessor() {
        return bulkProcessor;
    }

    @Override
    public void add(ActionRequest ar) {
        getBulkProcessor().add(ar);
    }

    @Override
    public void close() throws InterruptedException {
        boolean isClosed = false;
        while (!isClosed) {
            isClosed = bulkProcessor.awaitClose(2, TimeUnit.SECONDS);
        }
        bulkProcessor.close();
        client.close();
    }

    @Override
    public void flush(boolean await) {
        bulkProcessor.flush();
    }

    protected DsBulkProcessor.Listener createBulkProcessorListener() {
        return (DsBulkProcessor.Listener) getMainHook();
    }

    protected DsBulkProcessor createBulkProcessor(Client client, String bulkName) {
        DsBulkProcessor.Listener listener = createBulkProcessorListener();
        if (listener == null)
            return null;

        DsBulkProcessor.Builder builder = DsBulkProcessor.builder(
                client,
                listener
        );

        if (bulkActions != 0) {
            builder.setBulkActions(bulkActions);
        }
        if (flushInterval != null) {
            builder.setFlushInterval(flushInterval);
        }
//        else
//            builder.setFlushInterval(new TimeValue(20, TimeUnit.MINUTES));

        builder.setBulkSize(new ByteSizeValue(5, ByteSizeUnit.MB));
        builder.setBackoffPolicy(BackoffPolicy.exponentialBackoff());
        return builder.build();
    }
}
