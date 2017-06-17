package com.datastory.banyan.es;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.RhinoETLConsts;
import com.datastory.banyan.utils.BanyanTypeUtil;

/**
 * com.datastory.banyan.es.ESClientFactory
 *
 * @author lhfcws
 * @since 2017/2/10
 */
public class ESClientFactory {
    public static RhinoETLConfig conf = RhinoETLConfig.getInstance();
    public static final String CLUSTER_NAME = conf.get(RhinoETLConsts.ES_CLUSTER_NAME);
    public static final String[] ES_HOSTS = conf.getStrings(RhinoETLConsts.ES_HOSTS_BULK);

    public static SimpleEsBulkClient createOneNodeSimpleClient(String indexName, String indexType, int bulkNum) {
        String[] oneNode = {BanyanTypeUtil.randomPickArray(ES_HOSTS)};
        return new SimpleEsBulkClient(CLUSTER_NAME, indexName, indexType, oneNode, bulkNum);
//        return createSimpleClient(indexName, indexType, bulkNum);
    }

    public static SimpleEsBulkClient createSimpleClient(String indexName, String indexType, int bulkNum) {
        String[] esHosts = BanyanTypeUtil.shuffleCopyArray(ES_HOSTS);
        return new SimpleEsBulkClient(CLUSTER_NAME, indexName, indexType, esHosts, bulkNum);
    }

    public static ESBulkProcessorClient createESBulkProcessorClient(String indexName, String indexType, int bulkNum) {
        String[] esHosts = BanyanTypeUtil.shuffleCopyArray(ES_HOSTS);
        return new ESBulkProcessorClient(CLUSTER_NAME, indexName, indexType, esHosts, bulkNum);
    }

    public static BanyanEsBulkClient createBanyanEsBulkClient(String indexName, String indexType, int bulkNum) {
        String[] esHosts = BanyanTypeUtil.shuffleCopyArray(ES_HOSTS);
        return new BanyanEsBulkClient(CLUSTER_NAME, indexName, indexType, esHosts, bulkNum);
    }
}
