package com.datastory.banyan.weibo.hbase;

import com.datastory.banyan.base.Tables;
import com.datastory.banyan.doc.ResultRDocMapper;
import com.datastory.banyan.es.ESSearcher;
import com.datastory.banyan.hbase.HTableInterfacePool;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.google.common.base.Function;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

/**
 * com.datastory.banyan.weibo.hbase.WbUserReader
 *
 * @author lhfcws
 * @since 16/11/30
 */

public class WbUserReader implements Serializable {
    public static String INDEX_NAME = Tables.table(Tables.ES_WB_IDX);
    public static final String TYPE_WEIBO = "weibo";
    public static final String TWEETS = "_tweets";

    /**
     * not include fans
     * @param uid
     * @return
     * @throws Exception
     */
    public Params readUserNContent(String uid) throws Exception {
        List<String> mids = readMidsByUid(uid);
        Params up;
        List<Params> tweets = new LinkedList<>();

        HTableInterface hti = null;
        try {
            hti = HTableInterfacePool.get(Tables.table(Tables.PH_WBUSER_TBL));
            Get get = new Get(BanyanTypeUtil.wbuserPK(uid).getBytes());
            get.addFamily("r".getBytes());
            Result result = hti.get(get);
            up = new ResultRDocMapper(result).map();
        } finally {
            HTableInterfacePool.close(hti);
            hti = null;
        }

        try {
            hti = HTableInterfacePool.get(Tables.table(Tables.PH_WBCNT_TBL));
            List<Get> gets = new LinkedList<>();
            for (String mid : mids) {
                Get get = new Get(BanyanTypeUtil.wbcontentPK(mid).getBytes());
                gets.add(get);
            }
            Result[] results = hti.get(gets);

            for (Result result : results) {
                Params wbcontent = new ResultRDocMapper(result).map();
                tweets.add(wbcontent);
            }
        } finally {
            HTableInterfacePool.close(hti);
            hti = null;
        }

        up.put(TWEETS, tweets);
        return up;
    }

    public List<String> readMidsByUid(String uid) throws Exception {
        final List<String> ret = new LinkedList<>();
        QueryBuilder qb = new TermQueryBuilder("_routing", uid);
        ESSearcher searcher = ESSearcher.getInstance();
        TransportClient client = searcher.client();
        SearchRequestBuilder searchRequestBuilder = client
                .prepareSearch(INDEX_NAME)
                .setTypes(TYPE_WEIBO)
                .setSearchType(SearchType.SCAN)
                .setQuery(qb)
                .setFetchSource(new String[]{"mid"}, new String[]{});
        searchRequestBuilder.setScroll(new TimeValue(60000)).setSize(500);
        SearchResponse response = searchRequestBuilder.execute().actionGet();
        ESSearcher.getInstance().searchNScroll(response, new Function<SearchHit, Void>() {
            @Nullable
            @Override
            public Void apply(@Nullable SearchHit searchHitFields) {
                ret.add((String) searchHitFields.getSource().get("mid"));
                return null;
            }
        });
        return ret;
    }
}
