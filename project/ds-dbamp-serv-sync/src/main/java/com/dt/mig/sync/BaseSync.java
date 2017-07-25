package com.dt.mig.sync;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.base.MigSyncConfiguration;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.entity.EsReaderResult;
import com.dt.mig.sync.es.CommonReader;
import com.dt.mig.sync.extract.HighFreqyWordExtract;
import com.dt.mig.sync.sentiment.ISentiment;
import com.dt.mig.sync.sentiment.SentimentFactory;
import com.dt.mig.sync.sentiment.SentimentSourceType;
import com.dt.mig.sync.words.FilterWords;
import com.dt.mig.sync.words.IWords;
import com.dt.mig.sync.words.KeyWords;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by abel.chan on 16/11/18.
 */
@Deprecated
public abstract class BaseSync {

    private static final Logger LOG = LoggerFactory.getLogger(BaseSync.class);

    protected static final MigSyncConfiguration conf = MigSyncConfiguration.getInstance();

    protected static final CommonReader esReader = CommonReader.getInstance();

    protected ISentiment sentimentAnaly = null;

    protected HighFreqyWordExtract highFreqyWordExtract = null;

    protected BaseWriter handler;

    protected QueryBuilder queryBuilder = null;

    protected IWords keyWords = null;
    protected IWords filterWords = null;

    protected String esReadIndex;
    protected String esReadParentType;
    protected String esReadChildType;

    protected boolean exist = false;

    public BaseSync() {
    }

    public void setUp() {
        LOG.info("初始化参数开始");
        //关键词加载
        keyWords = new KeyWords();
        keyWords.setUp();

        //过滤词加载
        filterWords = new FilterWords();
        filterWords.setUp();

        //情感分析类初始化
        sentimentAnaly = SentimentFactory.buildSentimentObj(SentimentFactory.SentimentType.LongText);//长文本情感分析

        //高频词提取
        highFreqyWordExtract = new HighFreqyWordExtract();

        LOG.info("初始化参数完毕");
    }

    public void cleanUp() {

        LOG.info("回收资源开始");
        if (handler != null) {
            handler.cleanUp();
        }
        LOG.info("回收资源结束");
    }

    public void stop() {
        LOG.info("等待退出线程");
        this.exist = true;
        try {
            Thread.sleep(1000L * 10); //等待30s
        } catch (InterruptedException e) {
            LOG.error("error:{}", e);
        }
        LOG.info("退出线程结束..");
    }

    public void execute(String scrollId, BaseWriter.HandlerType type, SentimentSourceType sentimentSourceType, final boolean isSentimentAnaly, final boolean isExtractHighFreq, final boolean isCheckSentiment) throws Exception {
        LOG.info("开始执行es查询");
        if (StringUtils.isEmpty(scrollId)) {
            return;
        }
        while (!exist) {
            EsReaderResult result = esReader.search(scrollId);
            if (result == null) {
                throw new Exception("es获取结果有误!");
            }
            LOG.info("开始将es查询结果转换成YZDoc");
            List<YZDoc> docs = getYZDocList(result.getDatas(), type, sentimentSourceType, isSentimentAnaly, isExtractHighFreq);
            LOG.info("完成es结果转成YZDoc,共{}条", docs != null ? docs.size() : 0);

            LOG.info("开始将结果写入到新的es库");
            handler.write(docs, type, isCheckSentiment);
            LOG.info("docs资源已被回收!:docs:{}" + docs);

            boolean isEnd = result.isEnd();
            if (isEnd) {
                break;
            }
            scrollId = result.getScrollId();
        }
        LOG.info("同步任务执行完毕!");
    }

    /**
     * 将searchhit转成YZDoc,并进行情感判断
     *
     * @param hits
     * @param type
     * @param isSentimentAnaly 是否进行情感分析。
     * @return
     * @throws Exception
     */
    protected List<YZDoc> getYZDocList(SearchHit[] hits, BaseWriter.HandlerType type, SentimentSourceType sentimentSourceType, boolean isSentimentAnaly, boolean isExtractHighFreq) throws Exception {
        List<YZDoc> docs = handler.getYzDocList(hits, type);
        if (isSentimentAnaly) {
            sentimentAnaly(docs, MigSyncConsts.DEFAULT_ANALY_FIELD, sentimentSourceType);//根据关键词分析本体的情感;对每一个品牌,活动
        }
        if (isExtractHighFreq) {
            //提取高频词
            extractHighFreqyWord(docs, MigSyncConsts.DEFAULT_ANALY_FIELD);
        }
        return docs;
    }

    /**
     * 对doc的情感进行判断,根据keyword对每一个品牌,活动进行判断
     *
     * @param docs
     */
    protected void sentimentAnaly(List<YZDoc> docs, String field, SentimentSourceType sentimentSourceType) {
        sentimentAnaly.analysis(keyWords, field, MigSyncConsts.ES_FIELD_MIG_SENTIMENT, sentimentSourceType, docs);
    }

    /**
     * 对doc的内容高频词提取,并写到keywords里面
     *
     * @param docs
     */
    protected void extractHighFreqyWord(List<YZDoc> docs, String field) {
        highFreqyWordExtract.getHighFreqyWord(docs, field);
    }

    /**
     * 替换content字段中的话题,并分析其情感
     *
     * @param docs
     */
    public void removeTopicAnalySentiment(List<YZDoc> docs, String fieldKey, SentimentSourceType sentimentSourceType) {
        sentimentAnaly.removeTopicAnalysis(fieldKey, MigSyncConsts.ES_FIELD_SENTIMENT, sentimentSourceType, docs);
    }


    protected void initQueryBuilder(long start, long end) throws Exception {
        try {
            queryBuilder = buildQueryBuilder(start, end);
        } catch (Exception e) {
            throw e;
        }
    }

    public abstract void execute(long start, long end) throws Exception;

    protected abstract QueryBuilder buildQueryBuilder(long start, long end);

    protected abstract void executeParent() throws Exception;

    protected abstract void executeChild() throws Exception;

}
