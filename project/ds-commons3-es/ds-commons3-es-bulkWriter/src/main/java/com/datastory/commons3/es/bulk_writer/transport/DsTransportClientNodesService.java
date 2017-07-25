package com.datastory.commons3.es.bulk_writer.transport;

import org.apache.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.support.Headers;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.elasticsearch.client.transport.TransportClientNodesService;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicInteger;

public class DsTransportClientNodesService extends TransportClientNodesService {

    private final static Logger LOG = Logger.getLogger(DsTransportClientNodesService.class);

    private final TransportService transportService;
    private final AtomicInteger randomNodeGenerator;

    @Inject
    public DsTransportClientNodesService(Settings settings, ClusterName clusterName, TransportService transportService, ThreadPool threadPool, Headers headers, Version version) {
        super(settings, clusterName, transportService, threadPool, headers, version);
        this.transportService = transportService;
        try {
            Field field = TransportClientNodesService.class.getDeclaredField("randomNodeGenerator");
            field.setAccessible(true);
            randomNodeGenerator = (AtomicInteger) field.get(this);
        } catch (Exception e) {
            throw new RuntimeException(e); //impossible
        }
    }

    public <Response> void execute(NodeListenerCallback<Response> callback,
                                   ActionListener<Response> listener,
                                   DiscoveryNode preferredNode) {
        if (preferredNode == null) {
            execute(callback, listener);
        } else if (!transportService.nodeConnected(preferredNode)) {
            LOG.warn("execute, preferredNode not connected - " + preferredNode);
            execute(callback, listener);
        } else {
            List<DiscoveryNode> nodes = connectedNodes();
            ensureNodesAreAvailable(nodes);
            int index = getNodeNumber();
            RetryListener<Response> retryListener = new RetryListener<>(callback, listener, nodes, index);
            try {
                callback.doWithNode(preferredNode, retryListener);
            } catch (Throwable t) {
                //this exception can't come from the TransportService as it doesn't throw exception at all
                listener.onFailure(t);
            }
        }
    }

    private void ensureNodesAreAvailable(List<DiscoveryNode> nodes) {
        if (nodes.isEmpty()) {
            String message = String.format(Locale.ROOT, "None of the configured nodes are available: %s", listedNodes());
            throw new NoNodeAvailableException(message);
        }
    }

    private int getNodeNumber() {
        int index = randomNodeGenerator.incrementAndGet();
        if (index < 0) {
            index = 0;
            randomNodeGenerator.set(0);
        }
        return index;
    }
}
