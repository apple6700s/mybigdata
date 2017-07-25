package com.dt.mig.sync.questionAnswer;

import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.es.CommonQueryBuilder;
import com.dt.mig.sync.words.FilterDoc;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * com.dt.mig.sync.questionAnswer.QuestionAnswerSync
 *
 * @author zhaozhen
 * @since 2017/6/29
 */
public class QuestionAnswerSync extends AQuestionAnswerReader {

    private static final Logger LOG = LoggerFactory.getLogger(QuestionAnswerSync.class);

    public QuestionAnswerSync() {
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

        BoolQueryBuilder boolFilter = CommonQueryBuilder.buildQueryBuilder(Arrays.asList(SEARCH_ES_FILEDS), Arrays.asList(SEARCH_ES_FILEDS), keyWords, filterWords, FilterDoc.getInstance().getNewsForumDocId(), MigSyncConsts.ES_NEWS_QUESTION_POST_PUBLISH_TIME, start, end);
        QueryBuilder termQuery = QueryBuilders.termsQuery("cat_id", MigSyncConsts.ES_FIELDS_CAT_WENDA, MigSyncConsts.ES_FIELDS_CAT_BAIKE);
        boolFilter.filter(termQuery);
        return boolFilter;
    }
}
