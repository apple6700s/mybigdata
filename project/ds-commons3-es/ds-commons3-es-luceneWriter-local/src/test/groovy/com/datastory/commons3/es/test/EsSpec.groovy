package com.datastory.commons3.es.test

import com.datastory.commons3.es.lucene_writer.LocalIndexWriter
import com.datastory.commons3.es.lucene_writer.LocalIndexWriterFactory
import com.datastory.commons3.es.lucene_writer.RequestFactory
import org.elasticsearch.action.index.IndexRequest
import spock.lang.Specification
/**
 * com.datastory.commons3.es.test.EsSpec
 * Groovy specification class
 *
 * @author lhfcws
 * @since 2017/4/28
 */
class EsSpec extends Specification {
    static final String CLUSTER = "lhfcws_cluster";
    static final String INDEX = "ds-es";
    static final String TYPE = "weibo";

    /**
     * 直接在es集群写入的示例
     */
    def "local write data"() {
        when:
        System.properties.put("path.home", "/opt/package/elasticsearch-2.3.3");
        String index = INDEX;
        int shardId = 0;

        LocalIndexWriter writer = new LocalIndexWriterFactory().createLocalIndexWriter(index, shardId);

        for (int i = 0; i < 3; i++) {
            Map<String, String> params = FakeData.fake();
            IndexRequest indexRequest = RequestFactory.indexRequest(INDEX, TYPE, params);
            writer.write(indexRequest);
        }

        writer.flush();
        writer.close();

        then:
        true
    }
}
