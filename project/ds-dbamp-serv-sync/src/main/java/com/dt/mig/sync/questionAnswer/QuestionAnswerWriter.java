package com.dt.mig.sync.questionAnswer;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.BaseWriter;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.newsforum.NewsForumWriter;
import com.dt.mig.sync.questionAnswer.doc.NewsForumAnswerMapping;
import com.dt.mig.sync.questionAnswer.doc.NewsForumQuestionMapping;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * com.dt.mig.sync.questionAnswer.QuestionAnswerWriter
 *
 * @author zhaozhen
 * @since 2017/6/29
 */
public class QuestionAnswerWriter extends BaseWriter {

    private static final Logger LOG = LoggerFactory.getLogger(NewsForumWriter.class);

    public QuestionAnswerWriter() {
        super();
    }

    public void setUp() {
        //write
        this.esWriteIndex = MigSyncConsts.ES_NEWS_QUESTION_ANSWER_WRITER_INDEX;
        this.esWriteParentType = MigSyncConsts.ES_NEWS_QUESTION_PARENT_WRITE_TYPE;
        this.esWriteChildType = MigSyncConsts.ES_NEWS_ANSWER_CHILD_WRITE_TYPE;

        super.setUp();
    }

    @Override
    protected List<YZDoc> getParentYzDocList(SearchHit[] hits) throws Exception {
        List<YZDoc> docs = new ArrayList<YZDoc>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        for (SearchHit hit : hits) {
            Map<String, Object> metadata = hit.getSource();
            String id = hit.getId();
            YZDoc doc = new YZDoc(id);
            if (metadata != null) {
//                //LOG.info("[metadata COMMENT]: " + metadata);
//                //移除新库的publish_date,以免覆盖掉publish_time字段,
//                // 因为新es库的publish_date不是时间戳,而是字符串日期,yyyyMMddhhmmss
//                if (metadata.containsKey(MigSyncConsts.ES_NEWS_FORUM_COMMENT_PUBLISH_DATE)) {
//                    metadata.remove(MigSyncConsts.ES_NEWS_FORUM_COMMENT_PUBLISH_DATE);
//                }
                if (metadata.containsKey(MigSyncConsts.ES_NEWS_QUESTION_IS_DIGEST)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_QUESTION_IS_DIGEST);
                }
                if (metadata.containsKey(MigSyncConsts.ES_NEWS_QUESTION_POST_CAT_ID)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_QUESTION_POST_CAT_ID);
                }

                if (metadata.containsKey(MigSyncConsts.ES_NEWS_QUESTION_FINGERPRINT)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_QUESTION_FINGERPRINT);
                }
                if (metadata.containsKey(MigSyncConsts.ES_NEWS_QUESTION_IS_AD)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_QUESTION_IS_AD);
                }

                if (metadata.containsKey(MigSyncConsts.ES_NEWS_QUESTION_IS_RECOM)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_QUESTION_IS_RECOM);
                }

                if (metadata.containsKey(MigSyncConsts.ES_NEWS_QUESTION_IS_HOT)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_QUESTION_IS_HOT);
                }

                if (metadata.containsKey(MigSyncConsts.ES_NEWS_QUESTION_CONTENT_LEN)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_QUESTION_CONTENT_LEN);
                }

                if (metadata.containsKey(MigSyncConsts.ES_NEWS_QUESTION_TASK_ID)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_QUESTION_TASK_ID);
                }

                if (metadata.containsKey(MigSyncConsts.ES_NEWS_QUESTION_DISLIKE_CNT)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_QUESTION_DISLIKE_CNT);
                }

                if (metadata.containsKey(MigSyncConsts.ES_NEWS_QUESTION_IS_TOP)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_QUESTION_IS_TOP);
                }

                if (metadata.containsKey(MigSyncConsts.ES_NEWS_QUESTION_REVIEW_CNT)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_QUESTION_REVIEW_CNT);
                }

                if (metadata.containsKey(MigSyncConsts.ES_NEWS_QUESTION_VIEW_CNT)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_QUESTION_VIEW_CNT);
                }

                for (Map.Entry<String, Object> entry : metadata.entrySet()) {
                    // doc.put(entry.getKey(), entry.getValue());
                    doc.put(NewsForumQuestionMapping.getInstance().getNewKey(entry.getKey()), entry.getValue());
                }

            }

            //特殊处理,获取update_date ,将字符串日期转成long日期
            if (paramIsNotNull(doc, MigSyncConsts.ES_NEWS_QUESTION_POST_UPDATE_DATE)) {
                Long timestamp = getLongDate(sdf, doc.get(MigSyncConsts.ES_NEWS_QUESTION_POST_UPDATE_DATE).toString());
                doc.remove(MigSyncConsts.ES_NEWS_QUESTION_POST_UPDATE_DATE);
                if (timestamp != null) {
                    doc.put(MigSyncConsts.ES_NEWS_QUESTION_POST_UPDATE_DATE, timestamp);
                }
            }
            //特殊处理，获取publish_time，将字符串日期转为时间戳
//            if (paramIsNotNull(doc, MigSyncConsts.ES_NEWS_QUESTION_POST_PUBLISH_TIME)) {
//                Long timestamp = getLongDate(sdf, doc.get(MigSyncConsts.ES_NEWS_QUESTION_POST_PUBLISH_TIME).toString());
//                doc.remove(MigSyncConsts.ES_NEWS_QUESTION_POST_PUBLISH_TIME);
//                if (timestamp != null) {
//                    doc.put(MigSyncConsts.ES_NEWS_QUESTION_POST_PUBLISH_TIME, timestamp);
//                }
//            }

            LOG.info("[YZDOC PARENT]: " + doc.toJson());
            docs.add(doc);
        }

        //从hbase中获取content、以及title。
        this.hbaseClient.batchGetNewsForumQuestionPosts(docs);

        return docs;
    }

    private boolean paramIsNotNull(YZDoc doc, String field) {
        return doc != null && doc.containsKey(field) && doc.get(field) != null;
    }

    @Override
    protected List<YZDoc> getChildYzDocList(SearchHit[] hits) throws Exception {
        List<YZDoc> docs = new ArrayList<YZDoc>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        for (SearchHit hit : hits) {
            String parentId = hit.getFields().get("_parent").getValue();
            if (StringUtils.isEmpty(parentId)) continue;
            String id = hit.getId();
            Map<String, Object> metadata = hit.getSource();

            if (metadata != null) {

                YZDoc doc = new YZDoc(id);

                if (metadata.containsKey(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_CNT)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_CNT);
                }

                if (metadata.containsKey(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_CAT_ID)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_CAT_ID);
                }
                if (metadata.containsKey(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_FINGERPRINT)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_FINGERPRINT);
                }
                if (metadata.containsKey(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_IS_AD)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_IS_AD);
                }
                if (metadata.containsKey(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_CONTENT_LEN)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_CONTENT_LEN);
                }

                if (metadata.containsKey(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_TASK_ID)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_TASK_ID);
                }

                if (metadata.containsKey(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_DISLIKE_CNT)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_DISLIKE_CNT);
                }

                if (metadata.containsKey(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_VIEW_CNT)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_VIEW_CNT);
                }

//                if (metadata.containsKey(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_PARENT_POST_ID)) {
//                    metadata.remove(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_PARENT_POST_ID);
//                }

                for (Map.Entry<String, Object> entry : metadata.entrySet()) {
                    doc.put(NewsForumAnswerMapping.getInstance().getNewKey(entry.getKey()), entry.getValue());
                }
                doc.put("_parent", parentId);


                //特殊处理,获取update_date ,将字符串日期转成long日期
                if (paramIsNotNull(doc, MigSyncConsts.ES_NEWS_ANSWER_COMMENT_UPDATE_DATE)) {
                    Long timestamp = getLongDate(sdf, doc.get(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_UPDATE_DATE).toString());
                    doc.remove(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_UPDATE_DATE);
                    if (timestamp != null) {
                        doc.put(MigSyncConsts.ES_NEWS_ANSWER_COMMENT_UPDATE_DATE, timestamp);
                    }
                }

                LOG.info("[YZDOC CHILD]: " + doc.toJson());
                docs.add(doc);
            }
        }

        //从hbase中获取content和title
        this.hbaseClient.batchGetNewsForumAnswerComments(docs);

        return docs;
    }

    public Long getLongDate(SimpleDateFormat sdf, String value) {
        try {
            return sdf.parse(value).getTime();
        } catch (ParseException e) {

        }
        return null;
    }




}
