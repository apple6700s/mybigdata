package com.dt.mig.sync.questionAnswer;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.BaseReader;
import com.dt.mig.sync.BaseWriter;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.entity.EsReaderResult;
import com.dt.mig.sync.es.CommonQueryBuilder;
import com.dt.mig.sync.hbase.HBaseReader;
import com.dt.mig.sync.hbase.HBaseUtils;
import com.dt.mig.sync.sentiment.SentimentSourceType;
import com.yeezhao.commons.util.Entity.Params;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * com.dt.mig.sync.questionAnswer.AQuestionAnswerReader
 *
 * @author zhaozhen
 * @since 2017/6/29
 */
public abstract class AQuestionAnswerReader extends BaseReader {

    private static final Logger LOG = LoggerFactory.getLogger(AQuestionAnswerReader.class);

    public AQuestionAnswerReader() {
        super();
    }

    //需要build的字段
    public static final String[] SEARCH_ES_FILEDS = new String[]{MigSyncConsts.ES_NEWS_QUESTION_POST_CONTENT, MigSyncConsts.ES_NEWS_QUESTION_POST_TITLE};

    public void setUp() {

        //read
        this.esReadIndex = MigSyncConsts.ES_NEWS_QUESTION_ANSWER_INDEX;
        this.esReadParentType = MigSyncConsts.ES_NEWS_QUESTION_ANSWER_PARENT_TYPE;
        this.esReadChildType = MigSyncConsts.ES_NEWS_QUESTION_ANSWER_CHILD_TYPE;

        //将es结果转成yzdoc,并写入到es的hanler类。
        this.handler = new QuestionAnswerWriter();
        this.handler.setUp();

        super.setUp();
    }

    @Override
    protected void executeParent() throws Exception {
        boolean isSentimentAnaly = false;
        boolean isExtractHighFreq = false;
        boolean isCheckSentiment = true;

        EsReaderResult result = esReader.scroll(esReadIndex, esReadParentType, queryBuilder);
        if (result == null) {
            throw new Exception("es获取结果有误!");
        }

        BaseWriter.HandlerType type = BaseWriter.HandlerType.PARENT;
        List<YZDoc> docs = getYZDocList(result.getDatas(), type, SentimentSourceType.LONGTEXT, isSentimentAnaly, isExtractHighFreq);
        LOG.info("完成es结果转成YZDoc,共{}条", docs != null ? docs.size() : 0);

        buildNewsForumQuestionContent(docs);
        customSentimentAnaly(SentimentSourceType.LONGTEXT, docs);//根据关键词分析本体的情感;对每一个品牌,活动,自定义分析的情感函数
        //自定义提取content的高频词
        customExtractHighFreq(docs);
        SearchHit[] hits = result.getDatas();
        ArrayList<String> idCache = new ArrayList<>();
        for (SearchHit hit : hits) {
            idCache.add(hit.getId());
        }
//            拿取增量数据
        QuestionAnswerSyncTrend.getTrendDist(docs, idCache);
        LOG.info("[YZdoc]:"+ docs.iterator().next());
        handler.write(docs, type, isCheckSentiment);
        LOG.info("docs资源已被回收!:docs:{}" + docs);

        boolean isEnd = result.isEnd();
        if (!isEnd && !exist) {
            execute(result.getScrollId(), type, SentimentSourceType.LONGTEXT, isSentimentAnaly, isExtractHighFreq, isCheckSentiment);
        }
        LOG.info("news question_answer parent:post info finish sync!");
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
        List<YZDoc> docs = getYZDocList(result.getDatas(), type, SentimentSourceType.LONGTEXTCOMMENT, isSentimentAnaly, isExtractHighFreq);
        LOG.info("完成es结果转成YZDoc,共{}条", docs != null ? docs.size() : 0);
        buildNewsForumQuestionContent(docs);
        customSentimentAnaly(SentimentSourceType.LONGTEXT, docs);//根据关键词分析本体的情感;对每一个品牌,活动,自定义分析的情感函数
        //自定义提取content的高频词
        customExtractHighFreq(docs);

        SearchHit[] hits = result.getDatas();
        ArrayList<String> idCache = new ArrayList<>();
        for (SearchHit hit : hits) {
            idCache.add(hit.getId());
        }
//            拿取增量数据
        QuestionAnswerSyncTrend.getTrendDist(docs, idCache);
        handler.write(docs, type, isCheckSentiment);
        LOG.info("docs资源已被回收!:docs:{}" + docs);

        boolean isEnd = result.isEnd();
        if (!isEnd && !exist) {
            execute(result.getScrollId(), type, SentimentSourceType.LONGTEXTCOMMENT, isSentimentAnaly, isExtractHighFreq, isCheckSentiment);
        }
        LOG.info("news question_answer parent:comment info finish sync!");
    }

    @Override
    public void execute(String scrollId, BaseWriter.HandlerType type, SentimentSourceType sentimentSourceType, final boolean isSentimentAnaly, final boolean isExtractHighFreq, final boolean isCheckSentiment) throws Exception {
        LOG.info("开始执行es查询");
        if (StringUtils.isEmpty(scrollId)) {
            return;
        }
        while (true) {
            EsReaderResult result = esReader.search(scrollId);
            if (result == null) {
                throw new Exception("es获取结果有误!");
            }

            LOG.info("开始将es查询结果转换成YZDoc");
            List<YZDoc> docs = getYZDocList(result.getDatas(), type, sentimentSourceType, isSentimentAnaly, isExtractHighFreq);

            LOG.info("完成es结果转成YZDoc,共{}条", docs != null ? docs.size() : 0);
            if (type == BaseWriter.HandlerType.PARENT) {
                buildNewsForumQuestionContent(docs);
                customSentimentAnaly(SentimentSourceType.LONGTEXT, docs);//根据关键词分析本体的情感;对每一个品牌,活动,自定义分析的情感函数
                customExtractHighFreq(docs);//自定义提取高频词

                SearchHit[] hits = result.getDatas();
                ArrayList<String> idCache = new ArrayList<>();
                for (SearchHit hit : hits) {
                    idCache.add(hit.getId());
                }
//            拿取增量数据
                QuestionAnswerSyncTrend.getTrendDist(docs, idCache);
            }

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
     * 自定义高频词提取
     *
     * @param docs
     */
    protected void customExtractHighFreq(List<YZDoc> docs) {
        LOG.info("开始对doc的自定义高频词提取进行判断!");
        if (keyWords != null && docs != null && keyWords.getOriginWords() != null) {
            for (YZDoc doc : docs) {
                try {
                    //问答类使用content作为情感分析字段
                    String fieldKey = MigSyncConsts.DEFAULT_ANALY_FIELD;
                    highFreqyWordExtract.getHighFreqyWord(doc, fieldKey);
                } catch (Exception ex) {
                    LOG.error(ex.getMessage(), ex);
                }
            }
        }
        LOG.info("完成对doc的自定义高频词提取进行判断!");
    }

    /**
     * 自定义情感分析的函数
     *
     * @param docs
     */
    protected void customSentimentAnaly(SentimentSourceType sentimentSourceType, List<YZDoc> docs) {
        LOG.info("开始对doc的自定义情感进行判断!");
        if (keyWords != null && docs != null && keyWords.getOriginWords() != null) {
            for (YZDoc doc : docs) {
                try {

                    //获取需要分析的字段名称,默认是content。
                    String fieldKey = getAnalyFieldKey(doc);

                    sentimentAnaly.analysis(keyWords, fieldKey, MigSyncConsts.ES_FIELD_MIG_SENTIMENT, sentimentSourceType, doc);

                } catch (Exception ex) {
                    LOG.error(ex.getMessage(), ex);
                }
            }
        }
        LOG.info("完成对doc的自定义情感进行判断!");
    }

    /**
     * @param docs
     */
    public void buildNewsForumQuestionContent(List<YZDoc> docs) {
        for (YZDoc doc : docs) {
            try {
                //LOG.info("doc content:{}", doc.toJson());
                if (doc != null && doc.getId() != null && doc.containsKey(MigSyncConsts.ES_NEWS_QUESTION_POST_CAT_ID)) {

                    String catId = doc.get(MigSyncConsts.ES_NEWS_QUESTION_POST_CAT_ID).toString();
                    String content = "";
                    if (doc.containsKey(MigSyncConsts.ES_NEWS_QUESTION_POST_CONTENT) && doc.get(MigSyncConsts.ES_NEWS_QUESTION_POST_CONTENT) != null) {
                        content = doc.get(MigSyncConsts.ES_NEWS_QUESTION_POST_CONTENT).toString();
                    }
                    //根据catId知道分类，根据分类向content插入数据
                    if (MigSyncConsts.ES_FIELDS_CAT_WENDA.equals(catId) || MigSyncConsts.ES_FIELDS_CAT_BAIKE.equals(catId)) {

                        doc.put(MigSyncConsts.ES_NEWS_FORUM_POST_CONTENT, content);
                    }
                    //过滤掉html标签
//                    filterHtmlTag(doc);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 问答类，使用centent
     *
     * @param doc
     * @return
     */
    private String getAnalyFieldKey(YZDoc doc) {
        return MigSyncConsts.DEFAULT_ANALY_FIELD;
    }

    public QueryBuilder buildQueryBuilder(String id) {
        return CommonQueryBuilder.buildQueryBuilder(MigSyncConsts.ES_ROUTING_FIELD, id);
    }

}