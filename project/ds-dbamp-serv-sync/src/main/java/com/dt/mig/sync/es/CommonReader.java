package com.dt.mig.sync.es;

import com.dt.mig.sync.base.MigSyncConfiguration;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.entity.EsReaderResult;
import com.dt.mig.sync.utils.BanyanTypeUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.elasticsearch.action.count.CountRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AbstractAggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by abel.chan on 16/11/20.
 */
public class CommonReader {

    private static final Logger log = LoggerFactory.getLogger(CommonReader.class);

    private static CommonReader reader;

    private TransportClient client;

    private static int SCROLL_SIZE = 1000;

    private static int MAX_SCROLL_TIME_OUT = 3 * 60000;

    private CommonReader() {

        Configuration conf = MigSyncConfiguration.getInstance();

        String[] esHosts = BanyanTypeUtil.shuffleCopyArray(conf.getStrings(MigSyncConsts.ES_HOSTS));
        String clusterName = conf.get(MigSyncConsts.ES_CLUSTER_CONF);

        SCROLL_SIZE = Integer.parseInt(conf.get(MigSyncConsts.ES_SCROLL_SIZE, "1000"));

        log.info("esHosts: {} , clusterName: {}", StringUtils.join(esHosts, ","), clusterName);
        Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).put("client.transport.ping_timeout", "60s").build();
        TransportAddress[] transportAddresses = new InetSocketTransportAddress[esHosts.length];
        for (int i = 0; i < esHosts.length; i++) {
            String[] parts = esHosts[i].split(":");
            try {
                InetAddress inetAddress = InetAddress.getByName(parts[0]);
                transportAddresses[i] = new InetSocketTransportAddress(inetAddress, Integer.parseInt(parts[1]));
            } catch (UnknownHostException e) {
                log.error(e.getMessage(), e);
            }
        }
        client = TransportClient.builder().settings(settings).build().addTransportAddresses(transportAddresses);

    }

    public static CommonReader getInstance() {
        if (reader == null) {
            synchronized (CommonReader.class) {
                if (reader == null) {
                    reader = new CommonReader();
                }
            }
        }
        return reader;
    }

    public void close() {
        try {
            if (client != null) {
                client.close();
                Thread.sleep(2000);
                reader = null;
            }
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
    }

    /**
     * 获取满足query的总条数
     *
     * @param index
     * @param type
     * @param queryBuilder
     * @return
     */
    public long getTotalHits(String index, String type, QueryBuilder queryBuilder) throws Exception {
        CountRequestBuilder requestBuilder = client.prepareCount(index).setTypes(type).setQuery(queryBuilder);
        return requestBuilder.execute().actionGet().getCount();
    }

    public EsReaderResult searchForId(String index, String type, int from, int size, QueryBuilder builder) throws Exception {

        // 搜索条件
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch().setIndices(index).setTypes(type).setQuery(builder).setFrom(from).setSize(size).setFetchSource(new String[]{"id"}, new String[]{});

        SearchResponse response = searchRequestBuilder.execute().actionGet();

        return new EsReaderResult(null, response.getHits().getHits(), true);
    }

    public EsReaderResult search(String index, String type, int from, int size, QueryBuilder builder) throws Exception {

        // 搜索条件
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch().setIndices(index).setTypes(type).setQuery(builder).setFrom(from).setSize(size);

        SearchResponse response = searchRequestBuilder.execute().actionGet();

        return new EsReaderResult(null, response.getHits().getHits(), true);
    }

    public EsReaderResult scrollWithoutSource(String index, String type, int from, int size, QueryBuilder builder) throws Exception {

        // 搜索条件
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch().setIndices(index).setTypes(type).setQuery(builder).setFrom(from).setSize(size).setFetchSource(false).setScroll(new TimeValue(MAX_SCROLL_TIME_OUT));

        SearchResponse response = searchRequestBuilder.execute().actionGet();
        String scrollId = response.getScrollId();

        return new EsReaderResult(scrollId, response.getHits().getHits(), false);
    }


    /**
     * 使用scroll进行首次搜索
     */
    public EsReaderResult scroll(String index, String type, QueryBuilder builder) throws Exception {
        // 搜索条件
        log.info("执行es查询 index:{},type:{},query:\n{}", index, type, builder.toString().replaceAll("\n", ""));
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setQuery(builder).setSize(SCROLL_SIZE).setScroll(new TimeValue(MAX_SCROLL_TIME_OUT));

        // 执行
        SearchResponse searchResponse = searchRequestBuilder.get();
        String scrollId = searchResponse.getScrollId();

        //log.info("--------- searchByScroll scrollID {}", scrollId);
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        boolean isEnd = false;
        if (searchHits.length == 0) {
            isEnd = true;
        }
        return new EsReaderResult(scrollId, searchHits, isEnd);
    }

    /**
     * 使用scroll进行首次搜索
     */
    public EsReaderResult scroll(String index, String type, QueryBuilder builder, String[] includes, String[] excludes) throws Exception {
        // 搜索条件
        //log.info("执行es查询 index:{},type:{},query:\n{}", index, type, builder.toString().replaceAll("\n", ""));
        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index).setTypes(type).setQuery(builder).setSize(SCROLL_SIZE).setFetchSource(includes, excludes).setScroll(new TimeValue(MAX_SCROLL_TIME_OUT));

        // 执行
        SearchResponse searchResponse = searchRequestBuilder.get();
        String scrollId = searchResponse.getScrollId();

        //log.info("--------- searchByScroll scrollID {}", scrollId);
        SearchHit[] searchHits = searchResponse.getHits().getHits();
        boolean isEnd = false;
        if (searchHits.length == 0) {
            isEnd = true;
        }
        return new EsReaderResult(scrollId, searchHits, isEnd);
    }

    /**
     * 使用agg查询
     */
    public Aggregations aggSearch(String index, String type, QueryBuilder queryBuilder, AbstractAggregationBuilder aggregationBuilder) throws Exception {
        // 搜索条件
        //log.info("执行es查询 index:{},type:{},query:\n{}", index, type, builder.toString().replaceAll("\n", ""));
        log.info("执行es查询 index:{},type:{},query:\n{},\nagg:\n{}", index, type, queryBuilder.toString().replaceAll("\n", ""), aggregationBuilder.toString().replaceAll("\n", ""));
        SearchResponse searchRequestBuilder = client.prepareSearch(index).setTypes(type).setQuery(queryBuilder).addAggregation(aggregationBuilder).setSize(0).execute().actionGet();
        return searchRequestBuilder.getAggregations();
    }

    /**
     * 通过滚动ID获取文档
     *
     * @param scrollId
     */
    public EsReaderResult search(String scrollId) throws Exception {
        boolean isEnd = false;
        TimeValue timeValue = new TimeValue(MAX_SCROLL_TIME_OUT);
        SearchScrollRequestBuilder searchScrollRequestBuilder;
        SearchResponse response;
        // 结果
        log.info("--------- searchByScroll ---------");
        searchScrollRequestBuilder = client.prepareSearchScroll(scrollId);
        // 重新设定滚动时间
        searchScrollRequestBuilder.setScroll(timeValue);
        // 请求
        response = searchScrollRequestBuilder.get();
        SearchHit[] searchHits = response.getHits().getHits();
        // 每次返回下一个批次结果 直到没有结果返回时停止 即hits数组空时
        if (searchHits.length == 0) {
            isEnd = true;
        } else {
            // 更新scrollId
            scrollId = response.getScrollId();
        }

        return new EsReaderResult(scrollId, searchHits, isEnd);
    }
}
