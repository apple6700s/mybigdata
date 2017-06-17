package com.datastory.banyan.es;

import com.datastory.banyan.base.RhinoETLConfig;
import com.datastory.banyan.base.RhinoETLConsts;
import com.google.common.base.Function;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * com.datastory.banyan.es.ESSearcher
 *
 * @author lhfcws
 * @since 16/11/30
 */

public class ESSearcher implements Serializable {
    private TransportClient client;

    public ESSearcher() {
        initClient();
    }

    public ESSearcher(TransportClient client) {
        this.client = client;
    }

    private void initClient() {
        RhinoETLConfig conf = RhinoETLConfig.getInstance();
        String[] esHosts = conf.getStrings(RhinoETLConsts.ES_HOSTS_QUERY);
        String clusterName = conf.get(RhinoETLConsts.ES_CLUSTER_NAME);
        Settings settings = Settings.settingsBuilder().put("cluster.name", clusterName).build();
        TransportAddress[] transportAddresses = new InetSocketTransportAddress[esHosts.length];
        for (int i = 0; i < esHosts.length; i++) {
            String[] parts = esHosts[i].split(":");
            try {
                InetAddress inetAddress = InetAddress.getByName(parts[0]);
                transportAddresses[i] = new InetSocketTransportAddress(inetAddress, Integer.parseInt(parts[1]));
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
        }
        client = TransportClient.builder().settings(settings).build().addTransportAddresses(transportAddresses);
    }

    private static volatile ESSearcher _singleton = null;

    public static ESSearcher getInstance() {
        if (_singleton == null)
            synchronized (ESSearcher.class) {
                if (_singleton == null) {
                    _singleton = new ESSearcher();
                }
            }
        return _singleton;
    }

    public TransportClient client() {
        return client;
    }

    public static TransportClient getClient() {
        return getInstance().client();
    }

    public void close() {
        client.close();
        client = null;
    }

    /**
     * scroll & scan
     *
     * @param scrollId
     * @param searchHitHandler
     */
    public void scroll(String scrollId, Function<SearchHit, Void> searchHitHandler) {
        SearchResponse scrollResponse = null;
        do {
            scrollResponse = client.prepareSearchScroll(scrollId)
                    .setScroll(new TimeValue(60000)).execute().actionGet();
            if (scrollResponse == null || scrollResponse.getHits().getHits().length == 0) break;

            for (SearchHit hit : scrollResponse.getHits().getHits()) {
                searchHitHandler.apply(hit);
            }
        } while (true);
    }

    public String searchNScroll(SearchResponse response, Function<SearchHit, Void> searchHitHandler) {
        String scrollId = response.getScrollId();

        for (SearchHit hit : response.getHits().getHits()) {
            searchHitHandler.apply(hit);
        }

        scroll(scrollId, searchHitHandler);
        return scrollId;
    }

}
