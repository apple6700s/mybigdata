package com.dt.mig.sync.wechat;

import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.es.CommonQueryBuilder;
import com.dt.mig.sync.words.FilterDoc;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * com.dt.mig.sync.wechat.WechatUpdate
 *
 * @author zhaozhen
 * @since 2017/7/17
 */
public class WechatUpdate extends AWechatReader{

    private static final Logger LOG = LoggerFactory.getLogger(WechatUpdate.class);

    public WechatUpdate() {
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
//        executeParent();

        //child comment
        executeChild();
    }

    @Override
    public QueryBuilder buildQueryBuilder(long start, long end) {
        //使用新的关键词构建query
        return CommonQueryBuilder.buildQueryBuilder(Arrays.asList(SEARCH_ES_FILEDS), Arrays.asList(SEARCH_ES_FILEDS), newKeyWords, filterWords, FilterDoc.getInstance().getNewsForumDocId(), MigSyncConsts.ES_WECHAT_PUBLISH_TIME, start, end);

    }
}
