package com.dt.mig.sync.weibo;

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
public class WeiboWriter extends BaseWriter {

    private static final Logger LOG = LoggerFactory.getLogger(WeiboWriter.class);

    public WeiboWriter() {
        super();
    }

    public void setUp() {
        //write
        this.esWriteIndex = MigSyncConsts.ES_WEIBO_WRITER_INDEX;
        this.esWriteParentType = MigSyncConsts.ES_WEIBO_PARENT_WRITE_TYPE;
        this.esWriteChildType = MigSyncConsts.ES_WEIBO_CHILD_WRITE_TYPE;

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

        //从hbase中获取content、以及retweet_content。
        this.hbaseClient.batchGetWeibos(docs);

        //从hbase中获取uid的名称
        this.hbaseClient.batchGetUserNames(docs);

        //测试情感
//        for(YZDoc doc:docs){
//            doc.put("content","腾讯手机安全管家真好用,良心作品!手机安全管家就腾讯的好用。360手机卫士也是不错的选择!");
//        }

        return docs;
    }


}