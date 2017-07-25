package com.datastory.commons3.es.copyData;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.datastory.commons3.es.lucene_writer.ShardLocation;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Objects;


/**
 * com.datastory.commons3.es.copyData.EsIndexOperation
 *
 * @author zhaozhen
 * @since 2017/6/14
 */
public class EsIndexOperation {
    private static Log LOGGER = LogFactory.getLog(EsIndexOperation.class);


    public static TransportClient getTransportClient(String host, int port, String cluster_name) {
        Settings settings = Settings.settingsBuilder().put("cluster.name", cluster_name).put("client.transport.sniff", false).put("client.transport.ignore_cluster_name", true).build();
        TransportClient client = null;

        try {
            client = TransportClient.builder().settings(settings).build().addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
            System.out.println("success connect");
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return client;
    }

    public static boolean isIndexExists(TransportClient client, String index) {

        /**
         * TODO logger should be static outside
         */
        if (Objects.equals(client, null)) {
            LOGGER.info("--------- IndexAPI isIndexExists 请求客户端为null");
            return false;
        }
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        IndicesExistsResponse response = indicesAdminClient.prepareExists(index).get();
        return response.isExists();

    }

    public static void closeEsIndex(String host, int port, String cluster_name, String index) throws Exception {
        TransportClient client = EsIndexOperation.getTransportClient(host, port, cluster_name);
        closeEsIndex(client, index);
    }

    public static void openEsIndex(String host, int port, String cluster_name, String index) throws Exception {
        TransportClient client = EsIndexOperation.getTransportClient(host, port, cluster_name);
        openEsIndex(client, index);
    }

    public static boolean closeEsIndex(TransportClient client, String index) {
        IndicesAdminClient indicesAdminClient = client.admin().indices();
        CloseIndexResponse response = indicesAdminClient.prepareClose(index).get();
        return response.isAcknowledged();

    }

    public static boolean openEsIndex(TransportClient client, String index) {

        IndicesAdminClient indicesAdminClient = client.admin().indices();
        OpenIndexResponse response = indicesAdminClient.prepareOpen(index).get();
        return response.isAcknowledged();

    }

    //总shards数
    public static int totalShards(String host, int port, String cluster_name, String index) {
        ShardLocation shardInfo = new ShardLocation(host, index);
        try {
            shardInfo.refreshShards();
        } catch (Exception e) {
            e.printStackTrace();
        }
        TransportClient client = getTransportClient(host, port, cluster_name);
        GetSettingsResponse response = client.admin().indices().prepareGetSettings(index).get();
        Integer shards = null;
        for (ObjectObjectCursor<String, Settings> cursor : response.getIndexToSettings()) {
            Settings settings = cursor.value;
            shards = settings.getAsInt("index.number_of_shards", null);
        }
        int totalShards = shards;
        return totalShards;
    }

}
