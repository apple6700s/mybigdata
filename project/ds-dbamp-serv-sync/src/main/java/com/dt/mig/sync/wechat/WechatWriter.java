package com.dt.mig.sync.wechat;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.BaseWriter;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.wechat.doc.WechatMPMapping;
import com.dt.mig.sync.wechat.doc.WechatWechatMapping;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * com.dt.mig.sync.wechat.WechatWriter
 *
 * @author zhaozhen
 * @since 2017/7/13
 */
public class WechatWriter extends BaseWriter {

    private static final Logger LOG = LoggerFactory.getLogger(WechatWriter.class);

    public WechatWriter() {
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
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        for (SearchHit hit : hits) {
            Map<String, Object> metadata = hit.getSource();
            String id = hit.getId();
            YZDoc doc = new YZDoc(id);
            if (metadata != null) {

                for (Map.Entry<String, Object> entry : metadata.entrySet()) {

                    doc.put(WechatMPMapping.getInstance().getNewKey(entry.getKey()), entry.getValue());
                }
            }
         docs.add(doc);
        }
        return docs;
    }

    @Override
    protected List<YZDoc> getChildYzDocList(SearchHit[] hits) throws Exception {

        List<YZDoc> docs = new ArrayList<YZDoc>();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        for (SearchHit hit : hits) {
            Map<String, Object> metadata = hit.getSource();
            String id = hit.getId();
            YZDoc doc = new YZDoc(id);
            if (metadata != null) {

                for (Map.Entry<String, Object> entry : metadata.entrySet()) {

                    doc.put(WechatWechatMapping.getInstance().getNewKey(entry.getKey()), entry.getValue());
                }

            }
            docs.add(doc);
        }
        return docs;
    }


}