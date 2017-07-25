import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * PACKAGE_NAME.TestEs
 *
 * @author lhfcws
 * @since 2017/5/11
 */
public class TestEs {
    private TransportClient initClient() {
        String[] esHosts = new String[]{"localhost:9300"};
        String clusterName = "lhfcws_cluster";
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
        return TransportClient.builder().settings(settings).build().addTransportAddresses(transportAddresses);
    }

    @Test
    public void testJsonClient() {
        String query = "{\n" +
                "    \"query\": {\n" +
                "        \"term\": {\n" +
                "           \"_id\": {\n" +
                "              \"value\": \"20170502214732 | 21889\"\n" +
                "           }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        System.out.println(query);
        TransportClient client = initClient();

        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(query)) {
            BytesRef bs = parser.utf8Bytes();
            SearchRequestBuilder searchRequestBuilder = new SearchRequestBuilder(client, SearchAction.INSTANCE);
            SearchResponse searchResponse = searchRequestBuilder
                    .setIndices("ds-es").setQuery(query)
                    .execute().actionGet();
            System.out.println(searchResponse);
            System.out.println(searchResponse.getHits().hits()[0].sourceAsMap());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
