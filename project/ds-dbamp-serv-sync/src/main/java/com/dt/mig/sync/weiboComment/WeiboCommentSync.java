package com.dt.mig.sync.weiboComment;

import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.es.CommonQueryBuilder;
import com.dt.mig.sync.words.FilterDoc;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by abel.chan on 16/11/18.
 */
public class WeiboCommentSync extends AWeiboCommentReader {

    private static final Logger LOG = LoggerFactory.getLogger(WeiboCommentSync.class);

    public WeiboCommentSync() {
        super();
    }

    @Override
    public void execute(long start, long end) throws Exception {

        initQueryBuilder(start, end);//初始化builder

        //parent weibo
        executeParent();

        //child comment
        executeChild();

    }

    @Override
    public QueryBuilder buildQueryBuilder(long start, long end) {

        //TODO 微博评论整个都是mig项目在使用,所以直接全部迁移走即可。后期再优化
//        return CommonQueryBuilder.buildQueryBuilder(Arrays.asList(SEARCH_ES_FILEDS), Arrays.asList(SEARCH_ES_FILEDS), keyWords, filterWords, FilterDoc.getInstance().getWeiboDocId(), MigSyncConsts.ES_WEIBO_WEIBO_POST_TIME, start, end);
        return CommonQueryBuilder.buildQueryBuilder(null, null, null, null, FilterDoc.getInstance().getWeiboDocId(), MigSyncConsts.ES_WEIBO_WEIBO_POST_TIME, start, end);
    }

}
