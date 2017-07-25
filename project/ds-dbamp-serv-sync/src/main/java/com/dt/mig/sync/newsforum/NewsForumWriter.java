package com.dt.mig.sync.newsforum;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.ds.dbamp.core.utils.Md5Utils;
import com.dt.mig.sync.BaseWriter;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.newsforum.doc.NewsForumCommentMapping;
import com.dt.mig.sync.newsforum.doc.NewsForumPostMapping;
import com.dt.mig.sync.utils.BanyanTypeUtil;
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
 * Created by abel.chan on 16/11/18.
 */
public class NewsForumWriter extends BaseWriter {

    private static final Logger LOG = LoggerFactory.getLogger(NewsForumWriter.class);

    public NewsForumWriter() {
        super();
    }

    public void setUp() {
        //write
        this.esWriteIndex = MigSyncConsts.ES_NEWS_FORUM_WRITER_INDEX;
        this.esWriteParentType = MigSyncConsts.ES_NEWS_FORUM_PARENT_WRITE_TYPE;
        this.esWriteChildType = MigSyncConsts.ES_NEWS_FORUM_CHILD_WRITE_TYPE;

        super.setUp();
    }


    @Override
    protected List<YZDoc> getParentYzDocList(SearchHit[] hits) throws Exception {
        List<YZDoc> docs = new ArrayList<YZDoc>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");

        for (SearchHit hit : hits) {
            Map<String, Object> metadata = hit.getSource();
            String id = hit.getId();
            YZDoc doc = new YZDoc(id);
                if (metadata != null) {

                    //LOG.info("[metadata NEWS]: " + metadata);
                    //移除新库的publish_date,以免覆盖掉publish_time字段,
                    // 因为新es库的publish_date不是时间戳,而是字符串日期,yyyyMMddhhmmss
                    if (metadata.containsKey(MigSyncConsts.ES_NEWS_FORUM_POST_PUBLISH_DATE)) {
                        metadata.remove(MigSyncConsts.ES_NEWS_FORUM_POST_PUBLISH_DATE);
                    }
                    for (Map.Entry<String, Object> entry : metadata.entrySet()) {
                        // doc.put(entry.getKey(), entry.getValue());
                        doc.put(NewsForumPostMapping.getInstance().getNewKey(entry.getKey()), entry.getValue());
                    }
                }

//            if (doc.containsKey(MigSyncConsts.ES_NEWS_FORUM_POST_CAT_ID)) {
//                LOG.info("CAT_ID TYPE:" + doc.get(MigSyncConsts.ES_NEWS_FORUM_POST_CAT_ID).getClass().toString());
//            }
                //特殊处理,获取page_id ,url的MD5
                if (paramIsNotNull(doc, MigSyncConsts.ES_NEWS_FORUM_POST_URL)) {
                    doc.put(MigSyncConsts.ES_NEWS_FORUM_POST_PAGE_ID, Md5Utils.md5(doc.get(MigSyncConsts.ES_NEWS_FORUM_POST_URL).toString()));
                }

                if (paramIsNotNull(doc, MigSyncConsts.ES_NEWS_FORUM_POST_CAT_ID)) {
                    Short catId = BanyanTypeUtil.parseShort(doc.get(MigSyncConsts.ES_NEWS_FORUM_POST_CAT_ID));
                    if (catId != null) {
                        doc.put(MigSyncConsts.ES_NEWS_FORUM_POST_CAT_ID, catId);
                    }
                }

                //特殊处理,获取update_date ,将字符串日期转成long日期
                if (paramIsNotNull(doc, MigSyncConsts.ES_NEWS_FORUM_POST_UPDATE_DATE)) {
                    Long timestamp = getLongDate(sdf, doc.get(MigSyncConsts.ES_NEWS_FORUM_POST_UPDATE_DATE).toString());
                    doc.remove(MigSyncConsts.ES_NEWS_FORUM_POST_UPDATE_DATE);
                    if (timestamp != null) {
                        doc.put(MigSyncConsts.ES_NEWS_FORUM_POST_UPDATE_DATE, timestamp);
                    }
                }

//            LOG.info("[YZDOC NEWS]: " + doc.toJson());
                docs.add(doc);

            }

        //从hbase中获取content、以及title。
        this.hbaseClient.batchGetNewsForumPosts(docs);

        return docs;
    }

    private boolean paramIsNotNull(YZDoc doc, String field) {
        return doc != null && doc.containsKey(field) && doc.get(field) != null;
    }

    @Override
    protected List<YZDoc> getChildYzDocList(SearchHit[] hits) throws Exception {
        List<YZDoc> docs = new ArrayList<YZDoc>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");
        for (SearchHit hit : hits) {
            String parentId = hit.getFields().get("_parent").getValue();
            if (StringUtils.isEmpty(parentId)) continue;
            String id = hit.getId();
            Map<String, Object> metadata = hit.getSource();

            if (metadata != null) {

                //LOG.info("[metadata COMMENT]: " + metadata);
                //移除新库的publish_date,以免覆盖掉publish_time字段,
                // 因为新es库的publish_date不是时间戳,而是字符串日期,yyyyMMddhhmmss
                if (metadata.containsKey(MigSyncConsts.ES_NEWS_FORUM_COMMENT_PUBLISH_DATE)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_FORUM_COMMENT_PUBLISH_DATE);
                }
                if (metadata.containsKey(MigSyncConsts.ES_NEWS_FORUM_COMMENT_CAT_ID)) {
                    metadata.remove(MigSyncConsts.ES_NEWS_FORUM_COMMENT_CAT_ID);
                }
                YZDoc doc = new YZDoc(id);
                for (Map.Entry<String, Object> entry : metadata.entrySet()) {
                    doc.put(NewsForumCommentMapping.getInstance().getNewKey(entry.getKey()), entry.getValue());
                }
                doc.put("_parent", parentId);

                //特殊处理,获取page_id ,url的MD5
                if (paramIsNotNull(doc, MigSyncConsts.ES_NEWS_FORUM_COMMENT_URL)) {
                    doc.put(MigSyncConsts.ES_NEWS_FORUM_COMMENT_PAGE_ID, Md5Utils.md5(doc.get(MigSyncConsts.ES_NEWS_FORUM_COMMENT_URL).toString()));
                }

                //特殊处理,获取update_date ,将字符串日期转成long日期
                if (paramIsNotNull(doc, MigSyncConsts.ES_NEWS_FORUM_COMMENT_UPDATE_DATE)) {
                    Long timestamp = getLongDate(sdf, doc.get(MigSyncConsts.ES_NEWS_FORUM_COMMENT_UPDATE_DATE).toString());
                    doc.remove(MigSyncConsts.ES_NEWS_FORUM_COMMENT_UPDATE_DATE);
                    if (timestamp != null) {
                        doc.put(MigSyncConsts.ES_NEWS_FORUM_COMMENT_UPDATE_DATE, timestamp);
                    }
                }

//                LOG.info("[YZDOC COMMENT]: " + doc.toJson());
                docs.add(doc);
            }
        }

        //从hbase中获取content
        this.hbaseClient.batchGetNewsForumComments(docs);

        return docs;
    }

    public Long getLongDate(SimpleDateFormat sdf, String value) {
        try {
            return sdf.parse(value).getTime();
        } catch (ParseException e) {

        }
        return null;
    }

    @Override
    public String getALLComment(List<String> ids) throws Exception {
        return this.hbaseClient.batchGetNewsForumAllComment(ids);
    }


}
