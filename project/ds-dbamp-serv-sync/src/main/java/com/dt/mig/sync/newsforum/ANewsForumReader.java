package com.dt.mig.sync.newsforum;

import com.ds.dbamp.core.base.consts.es.DbampNewsForumEsConsts;
import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.BaseReader;
import com.dt.mig.sync.BaseWriter;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.entity.EsReaderResult;
import com.dt.mig.sync.es.CommonQueryBuilder;
import com.dt.mig.sync.sentiment.SentimentSourceType;
import com.dt.mig.sync.utils.SentenceUtil;
import com.yeezhao.commons.util.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * Created by abel.chan on 16/11/18.
 */
public abstract class ANewsForumReader extends BaseReader {

    private static final Logger LOG = LoggerFactory.getLogger(ANewsForumReader.class);

    public ANewsForumReader() {
        super();
    }

    //需要build的字段
    public static final String[] SEARCH_ES_FILEDS = new String[]{MigSyncConsts.ES_NEWS_FORUM_POST_ALL_CONTENT,
            //MigSyncConsts.ES_NEWS_FORUM_POST_CONTENT,
            MigSyncConsts.ES_NEWS_FORUM_POST_TITLE};

    @Override
    public void setUp() {

        //read
        this.esReadIndex = MigSyncConsts.ES_NEWS_FORUM_INDEX;
        this.esReadParentType = MigSyncConsts.ES_NEWS_FORUM_PARENT_TYPE;
        this.esReadChildType = MigSyncConsts.ES_NEWS_FORUM_CHILD_TYPE;

        //将es结果转成yzdoc,并写入到es的hanler类。
        this.handler = new NewsForumWriter();
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

        //根据id获取对应的子文档评论数据,用于拼接 content 字段
        buildNewsForumAllContent(docs);
        //此处需要才开始打,因为content是需要buildNewsForumAllContent调用后才用的。
        customSentimentAnaly(SentimentSourceType.LONGTEXT, docs);//根据关键词分析本体的情感;对每一个品牌,活动,自定义分析的情感函数
        //自定义提取content的高频词
        customExtractHighFreq(docs);
        handler.write(docs, type, isCheckSentiment);
        LOG.info("docs资源已被回收!:docs:{}" + docs);

        boolean isEnd = result.isEnd();
        if (!isEnd && !exist) {
            execute(result.getScrollId(), type, SentimentSourceType.LONGTEXT, isSentimentAnaly, isExtractHighFreq, isCheckSentiment);
        }
        LOG.info("news forum parent:post info finish sync!");
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

        handler.write(docs, type, isCheckSentiment);
        LOG.info("docs资源已被回收!:docs:{}" + docs);

        boolean isEnd = result.isEnd();
        if (!isEnd && !exist) {
            execute(result.getScrollId(), type, SentimentSourceType.LONGTEXTCOMMENT, isSentimentAnaly, isExtractHighFreq, isCheckSentiment);
        }
        LOG.info("news forum parent:comment info finish sync!");

    }

    /**
     * 自定义情感分析的函数,因为新闻论坛的逻辑不一样
     *
     * @param docs
     */
    protected void customSentimentAnaly(SentimentSourceType sentimentSourceType, List<YZDoc> docs) {
        LOG.info("开始对doc的自定义情感进行判断!");
        if (keyWords != null && docs != null && keyWords.getOriginWords() != null) {
            for (YZDoc doc : docs) {
                try {

                    //获取需要分析的字段名称,默认是content。当catId等于2时,或catId等于8且siteId等于101537时使用main_post
                    String fieldKey = getAnalyFieldKey(doc);

                    sentimentAnaly.analysis(keyWords, fieldKey, MigSyncConsts.ES_FIELD_MIG_SENTIMENT, sentimentSourceType, doc);
//                    LOG.info("[YZDOC COMMENT]: " + doc.toJson());
                } catch (Exception ex) {
                    LOG.error(ex.getMessage(), ex);
                }
            }
        }
        LOG.info("完成对doc的自定义情感进行判断!");
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
                    //获取需要分析的字段名称,默认是content。当catId等于2时,或catId等于8且siteId等于101537时使用main_post
                    String fieldKey = getAnalyFieldKey(doc);
                    highFreqyWordExtract.getHighFreqyWord(doc, fieldKey);
                } catch (Exception ex) {
                    LOG.error(ex.getMessage(), ex);
                }
            }
        }
        LOG.info("完成对doc的自定义高频词提取进行判断!");
    }

    /**
     * 贴吧(site_id=101537)以及论坛使用main_post进行分析,其他使用content
     *
     * @param doc
     * @return
     */
    private String getAnalyFieldKey(YZDoc doc) {
        String fieldKey = MigSyncConsts.DEFAULT_ANALY_FIELD;

        String catId = doc.get(MigSyncConsts.ES_NEWS_FORUM_POST_CAT_ID).toString();
        if (MigSyncConsts.ES_FIELDS_CAT_FORUM.equals(catId)) {
            //论坛,拿main_post作为情感分析字段
            fieldKey = MigSyncConsts.ES_NEWS_FORUM_POST_MAIN_POST;

        } else if (MigSyncConsts.ES_FIELDS_CAT_TIEBA.equals(catId)) {
            if (doc.containsKey(MigSyncConsts.ES_NEWS_FORUM_POST_SITE_ID) && doc.get(MigSyncConsts.ES_NEWS_FORUM_POST_SITE_ID) != null && DbampNewsForumEsConsts.TIEBA_SITE_ID.equals(doc.get(MigSyncConsts.ES_NEWS_FORUM_POST_SITE_ID).toString())) {
                //贴吧,site_id等于101537时,拿main_post作为情感分析字段
                fieldKey = MigSyncConsts.ES_NEWS_FORUM_POST_MAIN_POST;
            }
        }
        return fieldKey;
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
                //获取all content
                //根据id获取对应的子文档评论数据,用于拼接 content 字段
                buildNewsForumAllContent(docs);
                customSentimentAnaly(sentimentSourceType, docs);//根据关键词分析本体的情感;对每一个品牌,活动
                customExtractHighFreq(docs);//自定义提取高频词
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
     * @param docs
     */
    public void buildNewsForumAllContent(List<YZDoc> docs) {
        for (YZDoc doc : docs) {
            try {
                //LOG.info("doc content:{}", doc.toJson());
                if (doc != null && doc.getId() != null && doc.containsKey(MigSyncConsts.ES_NEWS_FORUM_POST_CAT_ID)) {
                    String id = doc.getId();
                    String catId = doc.get(MigSyncConsts.ES_NEWS_FORUM_POST_CAT_ID).toString();

                    String mainPost = "";
                    if (doc.containsKey(MigSyncConsts.ES_NEWS_FORUM_POST_MAIN_POST) && doc.get(MigSyncConsts.ES_NEWS_FORUM_POST_MAIN_POST) != null) {
                        mainPost = doc.get(MigSyncConsts.ES_NEWS_FORUM_POST_MAIN_POST).toString();
                    }
                    //LOG.info("mainPost:" + mainPost);
                    //根据catId知道分类，根据分类向content，mainpost插入数据
                    if (MigSyncConsts.ES_FIELDS_CAT_FORUM.equals(catId) || MigSyncConsts.ES_FIELDS_CAT_TIEBA.equals(catId)) {
                        //论坛，插入post和第一页所有comment的content，插入mainpost
                        List<String> commentIds = getPostAllCommentId(id);
                        String allComment = handler.getALLComment(commentIds);
                        String content = allComment != null ? mainPost + allComment : mainPost;
                        doc.put(MigSyncConsts.ES_NEWS_FORUM_POST_CONTENT, content);
                    } else if (MigSyncConsts.ES_FIELDS_CAT_NEWS.equals(catId)) { //新闻，插入post的content就可以

                        doc.put(MigSyncConsts.ES_NEWS_FORUM_POST_CONTENT, mainPost);
                        doc.remove(MigSyncConsts.ES_NEWS_FORUM_POST_MAIN_POST);//移除原先保存在main_post的数据
                        doc.remove(MigSyncConsts.ES_NEWS_FORUM_POST_MAIN_POST_LENGTH);//移除原先保存的main_post的长度

                    }
                    //过滤掉html标签
                    filterHtmlTag(doc);
                }
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
        }
    }

    public void filterHtmlTag(YZDoc doc) {
        if (doc.containsKey(MigSyncConsts.ES_NEWS_FORUM_POST_MAIN_POST) && doc.get(MigSyncConsts.ES_NEWS_FORUM_POST_MAIN_POST) != null) {
            //去除文字中的图片截屏内容
            String mainPost = SentenceUtil.removeImgPath(doc.get(MigSyncConsts.ES_NEWS_FORUM_POST_MAIN_POST).toString());
            mainPost = SentenceUtil.removeHtmlTag(mainPost);
            doc.put(MigSyncConsts.ES_NEWS_FORUM_POST_MAIN_POST, mainPost); // 过滤script标签
            if (mainPost != null) {
                doc.put(MigSyncConsts.ES_NEWS_FORUM_POST_MAIN_POST_LENGTH, mainPost.length());
            }
        }

        if (doc.containsKey(MigSyncConsts.ES_NEWS_FORUM_POST_CONTENT) && doc.get(MigSyncConsts.ES_NEWS_FORUM_POST_CONTENT) != null) {
            //去除文字中的图片截屏内容
            String content = SentenceUtil.removeImgPath(doc.get(MigSyncConsts.ES_NEWS_FORUM_POST_CONTENT).toString());
            content = SentenceUtil.removeHtmlTag(content);
            doc.put(MigSyncConsts.ES_NEWS_FORUM_POST_CONTENT, content); // 过滤script标签
            if (content != null) {
                doc.put(MigSyncConsts.ES_NEWS_FORUM_POST_CONTENT_LENGTH, content.length());
            }
        }
    }

    /**
     * 获取主贴下的所有评论id
     *
     * @param id
     */
    public List<String> getPostAllCommentId(String id) {
        try {
            QueryBuilder queryBuilder = buildQueryBuilder(id);

            long count = esReader.getTotalHits(esReadIndex, esReadChildType, queryBuilder);

            if (count > 0) {
                EsReaderResult esReaderResult = esReader.searchForId(esReadIndex, esReadChildType, 0, (int) count, queryBuilder);
                return getCommentIds(esReaderResult.getDatas());
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }

        return new ArrayList<String>();
    }

    public List<String> getCommentIds(SearchHit[] hits) {
        List<String> result = new ArrayList<String>();
        if (hits != null) {
            for (SearchHit hit : hits) {
                String id = hit.getId();
                if (!StringUtil.isNullOrEmpty(id)) {
                    result.add(id);
                }
            }
        }
        return result;
    }

    public QueryBuilder buildQueryBuilder(String id) {
        return CommonQueryBuilder.buildQueryBuilder(MigSyncConsts.ES_ROUTING_FIELD, id);
    }


}
