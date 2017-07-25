package com.datastory.commons3.es.bulk_writer.action;

import org.elasticsearch.cluster.node.DiscoveryNode;

public interface PreferredNodeActionRequest {
    DiscoveryNode getPreferredNode();
}
