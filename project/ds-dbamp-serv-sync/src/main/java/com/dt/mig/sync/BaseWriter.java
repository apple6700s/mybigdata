package com.dt.mig.sync;

import com.ds.dbamp.core.dao.es.YZDoc;
import com.dt.mig.sync.base.MigSyncConfiguration;
import com.dt.mig.sync.base.MigSyncConsts;
import com.dt.mig.sync.es.CommonWriter;
import com.dt.mig.sync.hbase.HBaseDataClient;
import com.dt.mig.sync.utils.SentimentUtil;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by abel.chan on 16/11/20.
 */
public abstract class BaseWriter {

    private static final Logger LOG = LoggerFactory.getLogger(BaseWriter.class);

    protected HBaseDataClient hbaseClient;

    protected static final MigSyncConfiguration conf = MigSyncConfiguration.getInstance();

    protected CommonWriter esParentWriter = null;
    protected CommonWriter esChildWriter = null;

    protected String esWriteIndex;
    protected String esWriteParentType;
    protected String esWriteChildType;

    public BaseWriter() {
    }

    public void setUp() {
        try {
            this.esParentWriter = new CommonWriter(conf, conf.get(MigSyncConsts.ES_CLUSTER_CONF), esWriteIndex, esWriteParentType);
            this.esChildWriter = new CommonWriter(conf, conf.get(MigSyncConsts.ES_CLUSTER_CONF), esWriteIndex, esWriteChildType);
            hbaseClient = HBaseDataClient.getInstance();

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    public List<YZDoc> getYzDocList(SearchHit[] hits, HandlerType type) throws Exception {
        if (type == HandlerType.PARENT) {
            return getParentYzDocList(hits);
        } else if (type == HandlerType.CHILD) {
            return getChildYzDocList(hits);
        } else {
            throw new Exception("handler不支持该类型!");
        }
    }

    /**
     * 写入到es,根据不同的文档类型,父文档,以及子文档。
     *
     * @param docs
     * @param type
     * @throws Exception
     */
    public void write(List<YZDoc> docs, HandlerType type, boolean isCheckSentiment) throws Exception {
        if (type == HandlerType.PARENT) {
            writeParent(docs, isCheckSentiment);
        } else if (type == HandlerType.CHILD) {
            writeChild(docs, isCheckSentiment);
        } else {
            throw new Exception("handler不支持该类型!");
        }
        try {
            //写完之后就回收docs资源。
            docs.clear();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    /**
     * 写数据到es的父文档。
     *
     * @param docs
     * @throws Exception
     */
    public void writeParent(List<YZDoc> docs, boolean isCheckSentiment) throws Exception {
        if (docs != null) {
            for (YZDoc doc : docs) {
                if (isCheckSentiment) {
                    SentimentUtil.checkAndSetSentiment(doc);
                }
                esParentWriter.addData(doc, IndexRequest.OpType.INDEX);
            }
        }
    }

    /**
     * 写数据到es的子文档,父文档的id存在在YZDoc中,_parent
     *
     * @param docs
     * @throws Exception
     */
    public void writeChild(List<YZDoc> docs, boolean isCheckSentiment) throws Exception {
        if (docs != null) {
            for (YZDoc doc : docs) {
                if (doc.containsKey("_parent")) {
                    if (isCheckSentiment) {
                        SentimentUtil.checkAndSetSentiment(doc);
                    }
                    String parentId = doc.remove("_parent").toString();
                    esChildWriter.addData(doc, parentId);
                }
            }
        }
    }

    /**
     * 获取所有的评论内容
     *
     * @param ids 评论内容的id
     * @throws Exception
     */
    public String getALLComment(List<String> ids) throws Exception {
        return "";//默认访问空,需要重载
    }

    public void cleanUp() {
        try {
            esParentWriter.flush();
            //移除parent writerkey
//            esParentWriter.removeWriter(conf.get(MigSyncConsts.ES_CLUSTER_CONF),
//                    esWriteIndex, esWriteParentType);
            esParentWriter.close();
            esChildWriter.flush();
            //移除parent writer
//            esChildWriter.removeWriter(conf.get(MigSyncConsts.ES_CLUSTER_CONF),
//                    esWriteIndex, esWriteChildType);
            esChildWriter.close();
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }


    protected abstract List<YZDoc> getParentYzDocList(SearchHit[] hits) throws Exception;

    protected abstract List<YZDoc> getChildYzDocList(SearchHit[] hits) throws Exception;

    /**
     * 枚举:父文档,子文档
     */
    public enum HandlerType {
        PARENT, CHILD
    }
}
