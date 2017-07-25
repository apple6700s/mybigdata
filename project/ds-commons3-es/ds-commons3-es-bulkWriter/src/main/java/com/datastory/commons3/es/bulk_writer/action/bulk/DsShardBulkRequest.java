package com.datastory.commons3.es.bulk_writer.action.bulk;

import com.datastory.commons3.es.bulk_writer.action.PreferredNodeActionRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;

public class DsShardBulkRequest extends BulkRequest implements PreferredNodeActionRequest {

    private final DiscoveryNode preferredNode;

    public DsShardBulkRequest() {
        this(null);
    }

    public DsShardBulkRequest(DiscoveryNode preferredNode) {
        this.preferredNode = preferredNode;
    }

    @Override
    public DiscoveryNode getPreferredNode() {
        return preferredNode;
    }
}
