package com.dt.mig.sync.wechat;

import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.es.CommonQueryBuilder;
import com.dt.mig.sync.words.FilterDoc;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.Arrays;

/**
 * com.dt.mig.sync.wechat.WechatSync
 *
 * @author zhaozhen
 * @since 2017/7/13
 */
public class WechatSync extends AWechatReader{

    public WechatSync() {super();}

    @Override
    public void execute(long start, long end) throws Exception{

        //初始化builder
        initQueryBuilder(start, end);

        //parent mp
//        executeParent();
        //child wechat
        executeChild();
    }

    @Override
    public QueryBuilder buildQueryBuilder(long start, long end) {

        BoolQueryBuilder boolFilter = CommonQueryBuilder.buildQueryBuilder(Arrays.asList(SEARCH_ES_FILEDS), Arrays.asList(SEARCH_ES_FILEDS), keyWords, filterWords, FilterDoc.getInstance().getNewsForumDocId(), MigSyncConsts.ES_WECHAT_PUBLISH_TIME, start, end);
        return boolFilter;
    }
}
