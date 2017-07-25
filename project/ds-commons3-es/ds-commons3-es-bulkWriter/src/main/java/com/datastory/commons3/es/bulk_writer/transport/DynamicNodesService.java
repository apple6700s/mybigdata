package com.datastory.commons3.es.bulk_writer.transport;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.datastory.commons3.es.bulk_writer.annotation.ThreadSafe;
import com.datastory.commons3.es.bulk_writer.utils.DsEsUtils;
import org.apache.http.Consts;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;

/**
 * 两个作用：
 * 1. 自动发现es data-node，并且每个30s检查是否有新增或者消失的node
 * 2. 观察IndexShard，通过index和shard获取对应的DiscoveryNode
 */
@ThreadSafe
public class DynamicNodesService {

    private static final Logger LOG = Logger.getLogger(DynamicNodesService.class);

    public static final String SETTING_DYNAMIC_ENABLED = "ds.dynamic.enabled";
    public static final String SETTING_HTTP_TIMEOUT = "ds.dynamic.httpTimeout";
    public static final String SETTING_HTTP_RETRIES = "ds.dynamic.httpRetries";
    public static final String SETTING_HTTP_ADDRESSES = "ds.dynamic.httpAddresses";
    public static final String SETTING_CONNECTED_NODES_MAXRATE = "ds.dynamic.connectedNodes.maxRate";
    public static final String SETTING_CONNECTED_NODES_MINNUM = "ds.dynaimic.connectedNodes.minNum";

    public static class Builder {

        private final DsTransportClient transportClient;

        private int httpTimeout = 10;
        private int httpRetries = 3;
        private Set<String> httpAddresses = new HashSet<>();

        private double connectedNodesMaxRate = 0.1;
        private int connectedNodesMinNum = 10;

        public Builder(DsTransportClient transportClient) {
            this.transportClient = transportClient;
        }

        public Builder setHttpTimeout(int httpTimeout) {
            this.httpTimeout = httpTimeout;
            return this;
        }

        public Builder setHttpRetries(int httpRetries) {
            this.httpRetries = httpRetries;
            return this;
        }

        public Builder addHttpAddress(String hostPort) {
            httpAddresses.add(hostPort);
            return this;
        }

        public Builder addHttpAddress(String host, int port) {
            httpAddresses.add(host + ":" + port);
            return this;
        }

        public Builder addHttpAddresses(String... hostPorts) {
            Collections.addAll(httpAddresses, hostPorts);
            return this;
        }

        public Builder setConnectedNodesMaxRate(double connectedNodesMaxRate) {
            this.connectedNodesMaxRate = connectedNodesMaxRate;
            return this;
        }

        public Builder setConnectedNodesMinNum(int connectedNodesMinNum) {
            this.connectedNodesMinNum = connectedNodesMinNum;
            return this;
        }

        public DynamicNodesService build() {
            assert transportClient != null: "transportClient is null";
            assert httpTimeout > 0: "httpTimeout <= 0";
            assert httpRetries > 0: "httpRetries <= 0";
            assert httpAddresses.size() > 0: "httpAddresses is empty";
            assert connectedNodesMaxRate > 0: "connectedNodesMaxRate <= 0";
            assert connectedNodesMinNum > 0: "connectedNodesMinNum <= 0";

            return new DynamicNodesService(transportClient,
                    httpTimeout, httpRetries, httpAddresses,
                    connectedNodesMaxRate, connectedNodesMinNum);
        }
    }

    private final DsTransportClient transportClient;
    private final ThreadPool threadPool;

    private final int httpTimeoutMs;
    private final int httpRetries;
    private final Set<String> httpAddresses = new HashSet<>();
    private final double connectedNodesMaxRate;
    private final int connectedNodesMinNum;

    private Map<String, Set<Integer>> watchedIndexShards = new HashMap<>();
    private Map<String, Map<Integer, DiscoveryNode>> watchedIndexShardNodes = new ConcurrentHashMap<>();

    private List<HttpRequestTask> tasks = new LinkedList<>();

    public DynamicNodesService(DsTransportClient transportClient,
                               Settings settings) {
        this.transportClient = transportClient;
        this.threadPool = transportClient.getThreadPool();

        this.httpTimeoutMs = settings.getAsInt(SETTING_HTTP_TIMEOUT, 10) * 1000;
        this.httpRetries = settings.getAsInt(SETTING_HTTP_RETRIES, 3);
        this.connectedNodesMaxRate = settings.getAsDouble(SETTING_CONNECTED_NODES_MAXRATE, 0.1);
        this.connectedNodesMinNum = settings.getAsInt(SETTING_CONNECTED_NODES_MINNUM, 10);

        String httpAddresses = settings.get(SETTING_HTTP_ADDRESSES);
        assert !DsEsUtils.isEmptyString(httpAddresses): "settings[" + SETTING_HTTP_ADDRESSES + "] is empty string";
        this.httpAddresses.addAll(Arrays.asList(httpAddresses.split(",")));

        threadPool.scheduleWithFixedDelay(createScheduledTask(true, null), TimeValue.timeValueSeconds(30));
    }

    private DynamicNodesService(DsTransportClient transportClient,
                               int httpTimeout,
                               int httpRetries,
                               Collection<String> httpAddresses,
                               double connectedNodesMaxRate,
                               int connectedNodesMinNum) {
        this.transportClient = transportClient;
        this.threadPool = transportClient.getThreadPool();

        this.httpTimeoutMs = httpTimeout * 1000;
        this.httpRetries = httpRetries;
        this.httpAddresses.addAll(httpAddresses);
        this.connectedNodesMaxRate = connectedNodesMaxRate;
        this.connectedNodesMinNum = connectedNodesMinNum;

        threadPool.scheduleWithFixedDelay(createScheduledTask(true, null), TimeValue.timeValueSeconds(30));
    }

    public void initDynamicNodes() throws InterruptedException, ExecutionException {
        ScheduledFuture<?> future;
        synchronized (this) {
            tasks.add(new NodesTask());
            future = threadPool.schedule(
                    TimeValue.timeValueMillis(0),
                    ThreadPool.Names.SAME, createScheduledTask(true, null));
        }
        future.get();
    }

    public void watchIndexShard(String indexName, Integer... shards) throws InterruptedException, ExecutionException {
        assert !DsEsUtils.isEmptyString(indexName): "indexName is empty string";
        assert shards.length > 0: "shards's length == 0";

        ScheduledFuture<?> future;
        synchronized (this) {
            Runnable runnable;
            if (watchedIndexShards.containsKey(indexName)) {
                Set<Integer> watchedShards = watchedIndexShards.get(indexName);
                List<Integer> filterShards = new ArrayList<>();
                for (int shard : shards) {
                    if (!watchedShards.contains(shard)) {
                        watchedShards.add(shard);
                    } else {
                        filterShards.add(shard);
                    }
                }
                if (!filterShards.isEmpty()) {
                    LOG.warn("already watched indexName(" + indexName + ")'s shards - " + filterShards);
                }
                runnable = createScheduledTask(false, null);
            } else {
                Set<Integer> watchedShards = new HashSet<>();
                Collections.addAll(watchedShards, shards);
                watchedIndexShards.put(indexName, watchedShards);

                final HttpRequestTask task = new IndexSearchShardsTask(indexName);
                tasks.add(task);
                runnable = createScheduledTask(true, task);
            }
            future = threadPool.schedule(TimeValue.timeValueMillis(0), ThreadPool.Names.SAME, runnable);
        }
        future.get();
    }

    public DiscoveryNode getIndexShardPrimaryNode(String indexName, int shard) {
        final Map<Integer, DiscoveryNode> shardNodes = watchedIndexShardNodes.get(indexName);
        return shardNodes != null ? shardNodes.get(shard) : null;
    }

    private Set<TransportAddress> allAddresses = new HashSet<>();
    private Map<String, Map<Integer, TransportAddress>> indexShardsAddresses = new HashMap<>();

    private Runnable createScheduledTask(final boolean runTasks, final HttpRequestTask task) {
        return new Runnable() {
            @Override
            public void run() {
                synchronized (DynamicNodesService.this) {
                    if (runTasks) {
                        if (task != null) {
                            if (allAddresses.isEmpty()) {
                                runHttpRequestTasks(new NodesTask(), task);
                            } else {
                                runHttpRequestTasks(task);
                            }
                        } else {
                            runHttpRequestTasks(tasks);
                        }
                    }
                    rebalanceConnectedNodes();
                }
            }
        };
    }

    private void rebalanceConnectedNodes() {
        if (allAddresses.isEmpty() && indexShardsAddresses.isEmpty()) {
            return;
        }
        Set<TransportAddress> newAddresses = new HashSet<>();
        Set<TransportAddress> listedAddresses = new HashSet<>();

        //将不在allAddresses中的node从client删除
        if (!transportClient.listedNodes().isEmpty()) {
            List<TransportAddress> removeNodes = new ArrayList<>();
            for (DiscoveryNode node : transportClient.listedNodes()) {
                if (!allAddresses.contains(node.address())) {
                    removeNodes.add(node.address());
                } else {
                    listedAddresses.add(node.address());
                }
            }
            for (TransportAddress address : removeNodes) {
                transportClient.removeTransportAddress(address);
            }
        }

        //将watch的indexShard加入client
        for (String indexName : indexShardsAddresses.keySet()) {
            Map<Integer, TransportAddress> shardsAddresses = indexShardsAddresses.get(indexName);
            Set<Integer> watchedShards = watchedIndexShards.get(indexName);

            for (Integer shardId : shardsAddresses.keySet()) {
                if (!watchedShards.contains(shardId)) {
                    continue;
                }
                TransportAddress address = shardsAddresses.get(shardId);
                if (allAddresses.contains(address) && !listedAddresses.contains(address)) {
                    newAddresses.add(address);
                }
            }
        }

        //计算client中期望的node数量
        int minSize = Math.min(connectedNodesMinNum, allAddresses.size());
        int expectedSize =
                Math.max(minSize, (int) (allAddresses.size() * connectedNodesMaxRate))
                        - newAddresses.size();
        //如果client当前node少于期望node，则随机从allAddresses中挑选补齐数量
        if (expectedSize > listedAddresses.size()) {
            List<TransportAddress> pickingAddresses = new LinkedList<>(allAddresses);
            pickingAddresses.removeAll(listedAddresses);
            pickingAddresses.removeAll(newAddresses);

            Random random = new Random();
            for (int i = listedAddresses.size(); i < expectedSize; i++) {
                newAddresses.add(pickingAddresses.remove(random.nextInt(pickingAddresses.size())));
            }
        }
        LOG.info("rebalance, newAddresses - " + newAddresses);
        transportClient.addTransportAddresses(newAddresses.toArray(new TransportAddress[0]));
        //连接完新增的node之后，构建IndexShard对应的DiscoveryNode
        for (String indexName : indexShardsAddresses.keySet()) {
            Map<Integer, TransportAddress> shardsAddresses = indexShardsAddresses.get(indexName);
            Set<Integer> watchedShards = watchedIndexShards.get(indexName);

            Map<Integer, DiscoveryNode> shardsNodes = new HashMap<>(shardsAddresses.size());
            for (Integer shardId : shardsAddresses.keySet()) {
                if (!watchedShards.contains(shardId)) {
                    continue;
                }
                TransportAddress address = shardsAddresses.get(shardId);
                for (DiscoveryNode node : transportClient.listedNodes()) {
                    if (address.equals(node.address())) {
                        shardsNodes.put(shardId, node);
                    }
                }
            }
            watchedIndexShardNodes.put(indexName, shardsNodes);
        }
    }

    private void runHttpRequestTasks(HttpRequestTask... tasks) {
        runHttpRequestTasks(Arrays.asList(tasks));
    }

    private void runHttpRequestTasks(List<HttpRequestTask> tasks) {
        HttpParams httpClientParams = new BasicHttpParams();
        HttpConnectionParams.setSoTimeout(httpClientParams, httpTimeoutMs);
        HttpConnectionParams.setConnectionTimeout(httpClientParams, httpTimeoutMs);

        Iterator<String> httpAddressesIter = httpAddresses.iterator();
        HttpClient httpClient = null;
        try {
            for (HttpRequestTask task : tasks) {
                int httpRetries = this.httpRetries;
                while (httpRetries-- > 0) {
                    if (!httpAddressesIter.hasNext()) {
                        httpAddressesIter = httpAddresses.iterator();
                    }
                    String httpAddress = httpAddressesIter.next();
                    HttpUriRequest request = task.createRequest(httpAddress);
                    if (request == null) {
                        break;
                    }
                    request.addHeader(HttpHeaders.CONNECTION, HTTP.CONN_KEEP_ALIVE);
                    String data = null;
                    try {
                        if (httpClient == null) {
                            httpClient = new DefaultHttpClient(httpClientParams);
                        }
                        HttpResponse response = httpClient.execute(request);
                        int statusCode = response.getStatusLine().getStatusCode();
                        if (statusCode == 200) {
                            data = EntityUtils.toString(response.getEntity(), Consts.UTF_8);
                            task.handleResponse(data);
                            break;
                        } else {
                            LOG.warn("http request(" + request + ") failed, statusCode - " + statusCode);
                        }
                    } catch (Throwable t) {
                        if (httpClient != null) {
                            httpClient.getConnectionManager().shutdown();
                            httpClient = null;
                        }
                        LOG.warn("http request(" + request + ") exception - " + data, t);
                    }
                }
            }
        } finally {
            if (httpClient != null) {
                httpClient.getConnectionManager().shutdown();
            }
        }
    }

    private interface HttpRequestTask {
        HttpUriRequest createRequest(String httpAddress);
        void handleResponse(String data) throws Exception;
    }

    private class NodesTask implements HttpRequestTask {

        @Override
        public HttpUriRequest createRequest(String httpAddress) {
            return new HttpGet("http://" + httpAddress + "/_nodes/http?nodes");
        }

        @Override
        public void handleResponse(String data) throws Exception {
            JSONObject json = JSON.parseObject(data);
            JSONObject nodes = json.getJSONObject("nodes");

            Set<TransportAddress> _allAddresses = new HashSet<>(nodes.size());
            for (String k : nodes.keySet()) {
                JSONObject node = nodes.getJSONObject(k);
                JSONObject attributes = node.getJSONObject("attributes");
                if (attributes == null || attributes.getBoolean("data") == null
                        || attributes.getBoolean("data")) {
                    TransportAddress address = DsEsUtils.parseTransportAddress(
                            node.getString("transport_address"));
                    _allAddresses.add(address);
                }
            }
            allAddresses = _allAddresses;
            LOG.info("NodesTask, allAddresses - " + _allAddresses);
        }
    }

    private class IndexSearchShardsTask implements HttpRequestTask {

        private final String indexName;

        public IndexSearchShardsTask(String indexName) {
            this.indexName = indexName;
        }

        @Override
        public HttpUriRequest createRequest(String httpAddress) {
            return new HttpGet("http://" + httpAddress + "/" + indexName + "/_search_shards");
        }

        @Override
        public void handleResponse(String data) throws Exception {
            JSONObject json = JSON.parseObject(data);
            JSONObject nodes = json.getJSONObject("nodes");
            JSONArray shards = json.getJSONArray("shards");

            Map<String, TransportAddress> nodeAddresses = new HashMap<>(nodes.size());
            for (String nodeId : nodes.keySet()) {
                JSONObject node = nodes.getJSONObject(nodeId);
                String transport_address = node.getString("transport_address");
                nodeAddresses.put(nodeId, DsEsUtils.parseTransportAddress(transport_address));
            }

            Map<Integer, TransportAddress> shardsAddresses = new HashMap<>(shards.size());
            for (int i = 0; i < shards.size(); i++) {
                JSONArray theShards = shards.getJSONArray(i);
                boolean found = false;

                for (int j = 0; j < theShards.size(); j++) {
                    JSONObject shard = theShards.getJSONObject(j);
                    int shardId = shard.getIntValue("shard");

                    if ("STARTED".equals(shard.getString("state")) && shard.getBooleanValue("primary")) {
                        String nodeId = shard.getString("node");
                        if (!nodeAddresses.containsKey(nodeId)) {
                            throw new Exception("index(" + indexName + ")'s " +
                                    "shard(" + shardId + ") primary-node(" + nodeId + ") " +
                                    "not exists in nodes");
                        }
                        shardsAddresses.put(shardId, nodeAddresses.get(nodeId));
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    LOG.warn("index(" + indexName + ")'s shard(" + i + ") no primary-node");
                }
            }
            LOG.info("IndexSearchShardsTask(" + indexName + "), shardsAddresses - " + shardsAddresses);
            indexShardsAddresses.put(indexName, shardsAddresses);
        }
    }
}
