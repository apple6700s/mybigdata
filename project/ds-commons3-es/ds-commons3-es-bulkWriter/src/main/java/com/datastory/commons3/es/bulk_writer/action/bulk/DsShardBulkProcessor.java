package com.datastory.commons3.es.bulk_writer.action.bulk;

import com.datastory.commons3.es.bulk_writer.annotation.UnThreadSafe;
import com.datastory.commons3.es.bulk_writer.transport.DsTransportClient;
import com.datastory.commons3.es.bulk_writer.transport.DynamicNodesService;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.DocumentRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;

/**
 * 直接单独批量写某个shard的数据，比如spark应用，先将数据hash，然后每个partition就直接写某个shard所在的node。
 * hash函数见：
 * @see com.datastory.commons3.es.bulk_writer.utils.DsEsUtils#generateShardId(String, String, int)
 */
@UnThreadSafe
public class DsShardBulkProcessor {

    private static final Logger LOG = Logger.getLogger(DsShardBulkProcessor.class);

    private static class ShardBulkRequestFactory extends DsBulkProcessor.BulkRequestFactory {

        final DynamicNodesService dynamicNodesService;
        final String indexName;
        final int shard;

        ShardBulkRequestFactory(DynamicNodesService dynamicNodesService, String indexName, int shard) {
            this.dynamicNodesService = dynamicNodesService;
            this.indexName = indexName;
            this.shard = shard;
        }

        @Override
        public BulkRequest bulkRequest() {
            DiscoveryNode preferredNode =
                    dynamicNodesService.getIndexShardPrimaryNode(indexName, shard);
            LOG.info("bulkRequest, index(" + indexName + "), " +
                    "shard(" + shard + ") ,preferredNode(" + preferredNode + ")");
            return new DsShardBulkRequest(preferredNode)
                    .timeout(timeout)
                    .refresh(refresh)
                    .consistencyLevel(consistencyLevel);
        }
    }

    public static class Builder {

        private final String indexName;
        private final DsBulkProcessor.Builder bulkProcessorBuilder;

        public Builder(DsTransportClient client, final String indexName, final int shard) {
            final DynamicNodesService dynamicNodesService = client.getDynamicNodesService();
            if (dynamicNodesService == null) {
                throw new IllegalArgumentException("client's dynamicNodesService is null");
            }

            this.indexName = indexName;
            this.bulkProcessorBuilder = new DsBulkProcessor.Builder(client) {
                @Override
                protected DsBulkProcessor.BulkRequestFactory createBulkRequestFactory() {
                    return new ShardBulkRequestFactory(dynamicNodesService, indexName, shard);
                }
            };
        }

        public DsBulkProcessor.Builder getBulkProcessorBuilder() {
            return bulkProcessorBuilder;
        }

        public DsShardBulkProcessor build() {
            DsBulkProcessor bulkProcessor = bulkProcessorBuilder.build();
            return new DsShardBulkProcessor(bulkProcessor, indexName);
        }
    }

    private final DsBulkProcessor bulkProcessor;
    private final String indexName;

    private DsShardBulkProcessor(DsBulkProcessor bulkProcessor, String indexName) {
        this.bulkProcessor = bulkProcessor;
        this.indexName = indexName;
    }

    public String getIndexName() {
        return indexName;
    }

    /**
     * @throws IllegalArgumentException
     */
    public DsShardBulkProcessor addWithValidate(ActionRequest request) throws IllegalArgumentException {
        if (request instanceof DocumentRequest) {
            DocumentRequest documentRequest = (DocumentRequest) request;
            if (!indexName.equals(documentRequest.index())) {
                throw new IllegalArgumentException("request's index(" + documentRequest.index() + ") " +
                        "!= bulkProcessor's index(" + indexName + ")");
            }
        }
        bulkProcessor.addWithValidate(request);
        return this;
    }

    public DsBulkResponse flush() throws IllegalArgumentException, InterruptedException {
        return bulkProcessor.flush();
    }
}
