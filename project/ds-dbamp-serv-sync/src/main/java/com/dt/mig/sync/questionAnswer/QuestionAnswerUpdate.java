package com.dt.mig.sync.questionAnswer;

import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.es.CommonQueryBuilder;
import com.dt.mig.sync.newsforum.NewsForumUpdate;
import com.dt.mig.sync.words.FilterDoc;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * com.dt.mig.sync.questionAnswer.QuestionAnswerUpdate
 *
 * @author zhaozhen
 * @since 2017/6/29
 */
public class QuestionAnswerUpdate extends AQuestionAnswerReader {

    private static final Logger LOG = LoggerFactory.getLogger(NewsForumUpdate.class);

    public QuestionAnswerUpdate() {
        super();
    }

    @Override
    public void execute(long start, long end) throws Exception {

        if (newKeyWords == null) {
            LOG.error("需要更新的关键词不能为空!");
            System.exit(1);
        }

        initQueryBuilder(start, end);//初始化builder

        //parent post
        executeParent();

        //child comment
        executeChild();
    }

    @Override
    public QueryBuilder buildQueryBuilder(long start, long end) {
        //使用新的关键词构建query
//        return CommonQueryBuilder.buildQueryBuilder(Arrays.asList(SEARCH_ES_FILEDS), Arrays.asList(SEARCH_ES_FILEDS), newKeyWords, filterWords, FilterDoc.getInstance().getNewsForumDocId(), MigSyncConsts.ES_NEWS_FORUM_POST_PUBLISH_TIME, start, end);
        BoolQueryBuilder boolFilter = CommonQueryBuilder.buildQueryBuilder(Arrays.asList(SEARCH_ES_FILEDS), Arrays.asList(SEARCH_ES_FILEDS), keyWords, filterWords, FilterDoc.getInstance().getNewsForumDocId(), MigSyncConsts.ES_NEWS_QUESTION_POST_PUBLISH_TIME, start, end);
        QueryBuilder termQuery = QueryBuilders.termsQuery("cat_id", MigSyncConsts.ES_FIELDS_CAT_WENDA, MigSyncConsts.ES_FIELDS_CAT_BAIKE);
        boolFilter.filter(termQuery);
        return boolFilter;
    }
}
