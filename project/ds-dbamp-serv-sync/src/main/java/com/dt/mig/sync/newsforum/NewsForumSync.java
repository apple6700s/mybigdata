package com.dt.mig.sync.newsforum;

import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.es.CommonQueryBuilder;
import com.dt.mig.sync.words.FilterDoc;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;


/**
 * Created by abel.chan on 16/11/18.
 */
public class NewsForumSync extends ANewsForumReader {

    private static final Logger LOG = LoggerFactory.getLogger(NewsForumSync.class);

    public NewsForumSync() {
        super();
    }

    @Override
    public void execute(long start, long end) throws Exception {

        initQueryBuilder(start, end);//初始化builder

        //parent post
        executeParent();

        //child comment
        executeChild();
    }

    @Override
    public QueryBuilder buildQueryBuilder(long start, long end) {

        BoolQueryBuilder boolFilter = CommonQueryBuilder.buildQueryBuilder(Arrays.asList(SEARCH_ES_FILEDS), Arrays.asList(SEARCH_ES_FILEDS), keyWords, filterWords, FilterDoc.getInstance().getNewsForumDocId(), MigSyncConsts.ES_NEWS_FORUM_POST_PUBLISH_TIME, start, end);
        QueryBuilder termQuery = QueryBuilders.termsQuery("cat_id", MigSyncConsts.ES_FIELDS_CAT_FORUM, MigSyncConsts.ES_FIELDS_CAT_NEWS);
        QueryBuilder termQuery1 = QueryBuilders.termQuery("cat_id", MigSyncConsts.ES_FIELDS_CAT_TIEBA);
        QueryBuilder termQuery2 = QueryBuilders.termQuery("site_id",MigSyncConsts.ES_FIELDS_SITE_TIEBA);
        QueryBuilder tiebaQuery = QueryBuilders.boolQuery().must(termQuery1).must(termQuery2);
        boolFilter.should(termQuery);
        boolFilter.should(tiebaQuery);
        boolFilter.minimumNumberShouldMatch(1);
        return boolFilter;
    }

}
