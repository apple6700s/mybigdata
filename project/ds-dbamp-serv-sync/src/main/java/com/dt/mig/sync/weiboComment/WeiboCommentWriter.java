package com.dt.mig.sync.weiboComment;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.BaseWriter;
import com.dt.mig.sync.base.MigSyncConsts;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by abel.chan on 16/11/18.
 */
public class WeiboCommentWriter extends BaseWriter {

    private static final Logger LOG = LoggerFactory.getLogger(WeiboCommentWriter.class);

    public WeiboCommentWriter() {
        super();
    }

    public void setUp() {
        //write
        this.esWriteIndex = MigSyncConsts.ES_WEIBO_COMMENT_WRITER_INDEX;
        this.esWriteParentType = MigSyncConsts.ES_WEIBO_COMMENT_PARENT_WRITE_TYPE;
        this.esWriteChildType = MigSyncConsts.ES_WEIBO_COMMENT_CHILD_WRITE_TYPE;

        super.setUp();
    }


    @Override
    protected List<YZDoc> getParentYzDocList(SearchHit[] hits) throws Exception {
        List<YZDoc> docs = new ArrayList<YZDoc>();

        for (SearchHit hit : hits) {
            Map<String, Object> metadata = hit.getSource();
            String id = hit.getId();
            YZDoc doc = new YZDoc(id);
            if (metadata != null) {
                for (Map.Entry<String, Object> entry : metadata.entrySet()) {
                    doc.put(entry.getKey(), entry.getValue());
                }
            }
            docs.add(doc);
        }

        //从hbase中获取content、以及retweet_content,以及uid。
        this.hbaseClient.batchGetWCWeibos(docs);

        return docs;
    }

    @Override
    protected List<YZDoc> getChildYzDocList(SearchHit[] hits) throws Exception {
        List<YZDoc> docs = new ArrayList<YZDoc>();

        for (SearchHit hit : hits) {
            String parentId = hit.getFields().get(MigSyncConsts.PARENT_ID).getValue();
            if (StringUtils.isEmpty(parentId)) continue;
            String id = hit.getId();
            Map<String, Object> metadata = hit.getSource();
            if (metadata != null) {
                YZDoc doc = new YZDoc(id);
                for (Map.Entry<String, Object> entry : metadata.entrySet()) {
                    doc.put(entry.getKey(), entry.getValue());
                }
                doc.put(MigSyncConsts.PARENT_ID, parentId);
                docs.add(doc);
            }
        }

        //从hbase获取评论的content
        this.hbaseClient.batchGetWCComments(docs);

        //从hbase获取评论对应的weibo的mid、retweet_id、msg_type
        this.hbaseClient.batchGetExtraWCComments(docs);

        return docs;
    }


}
