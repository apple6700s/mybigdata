package com.dt.mig.sync.weiboComment;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.BaseReader;
import com.dt.mig.sync.BaseWriter;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.entity.EsReaderResult;
import com.dt.mig.sync.sentiment.SentimentSourceType;
import org.elasticsearch.index.query.QueryBuilders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by abel.chan on 16/11/18.
 */
public abstract class AWeiboCommentReader extends BaseReader {

    private static final Logger LOG = LoggerFactory.getLogger(AWeiboCommentReader.class);

    public AWeiboCommentReader() {
        super();
    }

    public static final String[] SEARCH_ES_FILEDS = new String[]{MigSyncConsts.ES_WEIBO_WEIBO_RETWEET_CONTENT, MigSyncConsts.ES_WEIBO_WEIBO_CONTENT};

    @Override
    public void setUp() {

        //read
        this.esReadIndex = MigSyncConsts.ES_WEIBO_CONMENT_INDEX;
        this.esReadParentType = MigSyncConsts.ES_WEIBO_CONMENT_PARENT_TYPE;
        this.esReadChildType = MigSyncConsts.ES_WEIBO_CONMENT_CHILD_TYPE;

        //将es结果转成yzdoc,并写入到es的hanler类。
        this.handler = new WeiboCommentWriter();
        this.handler.setUp();


        super.setUp();
    }

    @Override
    protected void executeParent() throws Exception {
        boolean isSentimentAnaly = true;
        boolean isExtractHighFreq = false;
        boolean isCheckSentiment = true;
        EsReaderResult result = esReader.scroll(esReadIndex, esReadParentType, queryBuilder);
        if (result == null) {
            throw new Exception("es获取结果有误!");
        }

        BaseWriter.HandlerType type = BaseWriter.HandlerType.PARENT;
        List<YZDoc> docs = getYZDocList(result.getDatas(), type, SentimentSourceType.WEIBO, isSentimentAnaly, isExtractHighFreq);
        LOG.info("完成es结果转成YZDoc,共{}条", docs != null ? docs.size() : 0);

        //剔除微博话题重新判断sentiment
        removeTopicAnalySentiment(docs, MigSyncConsts.DEFAULT_ANALY_FIELD, SentimentSourceType.WEIBO);

        handler.write(docs, type, isCheckSentiment);
        LOG.info("docs资源已被回收!:docs:{}" + docs);

        boolean isEnd = result.isEnd();
        if (!isEnd && !exist) {
            execute(result.getScrollId(), type, SentimentSourceType.WEIBO, isSentimentAnaly, isExtractHighFreq, isCheckSentiment);
        }
        LOG.info("weibo comment parent:weibo info finish sync!");
    }

    @Override
    protected void executeChild() throws Exception {
        boolean isSentimentAnaly = true;
        boolean isExtractHighFreq = true;
        boolean isCheckSentiment = true;
        EsReaderResult result = esReader.scroll(esReadIndex, esReadChildType, QueryBuilders.hasParentQuery(esReadParentType, queryBuilder));
        if (result == null) {
            throw new Exception("es获取结果有误!");
        }

        BaseWriter.HandlerType type = BaseWriter.HandlerType.CHILD;
        List<YZDoc> docs = getYZDocList(result.getDatas(), type, SentimentSourceType.WEIBOCOMMENT, isSentimentAnaly, isExtractHighFreq);
        LOG.info("完成es结果转成YZDoc,共{}条", docs != null ? docs.size() : 0);

        handler.write(docs, type, isCheckSentiment);
        LOG.info("docs资源已被回收!:docs:{}" + docs);

        boolean isEnd = result.isEnd();
        if (!isEnd && !exist) {
            execute(result.getScrollId(), type, SentimentSourceType.WEIBOCOMMENT, isSentimentAnaly, isExtractHighFreq, isCheckSentiment);
        }
        LOG.info("weibo comment parent:comment info finish sync!");
    }

}
