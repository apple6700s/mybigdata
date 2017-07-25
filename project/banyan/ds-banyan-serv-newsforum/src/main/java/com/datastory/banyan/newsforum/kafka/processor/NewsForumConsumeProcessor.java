package com.datastory.banyan.newsforum.kafka.processor;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.batch.CountUpLatchBlockProcessor;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.monitor.stat.AccuStat;
import com.datastory.banyan.monitor.stat.ETLStatWrapper;
import com.datastory.banyan.newsforum.analyz.NewsForumAnalyzer;
import com.datastory.banyan.newsforum.doc.Rhino2NewsForumDocMapper;
import com.datastory.banyan.newsforum.hbase.PhoenixNewsForumCmtWriter;
import com.datastory.banyan.newsforum.hbase.PhoenixNewsForumPostWriter;
import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.utils.DateUtils;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * com.datastory.banyan.newsforum.kafka.processor.NewsForumConsumeProcessor
 *
 * @author lhfcws
 * @since 2017/7/5
 */
public class NewsForumConsumeProcessor extends CountUpLatchBlockProcessor {
    AtomicInteger docSize = new AtomicInteger(0);
    ETLStatWrapper stat = new ETLStatWrapper();
    NewsForumAnalyzer analyzer = NewsForumAnalyzer.getInstance();

    PhoenixNewsForumCmtWriter cmtWriter = PhoenixNewsForumCmtWriter.getInstance(2000);
    PhoenixNewsForumPostWriter postWriter = PhoenixNewsForumPostWriter.getInstance(2000);


    public NewsForumConsumeProcessor(CountUpLatch latch) {
        super(latch);
    }

    @Override
    public void _process(Object _p) {
        try {
            JSONObject jsonObject = (JSONObject) _p;

            List<Params> list = new Rhino2NewsForumDocMapper(jsonObject).map();

            if (CollectionUtil.isEmpty(list)) return;

            // all_content
            Params mainPost = list.get(0);
            if (!analyzer.isMainPost(mainPost))
                mainPost = null;

            if (mainPost != null) {
                StringBuilder allContent = new StringBuilder();
                if (mainPost.getString("content") != null)
                    allContent.append(mainPost.getString("content"));

                for (int i = 1; i < list.size(); i++) {
                    Params p = list.get(i);
                    if (p.get("content") != null)
                        allContent.append(p.getString("content"));
                }
                mainPost.put("all_content", allContent.toString());
            }

            // analyz
            for (int i = 0; i < list.size(); i++) {
                try {
                    Params p = list.get(i);
                    LOG.info(p.getString("pk") + " , pdate: " + p.getString("publish_date") + ", udate: " + p.getString("update_date"));
                    AccuStat s;
                    s = stat.analyzStat().tsafeBegin();
                    p = analyzer.analyz(p);
                    stat.analyzStat().tsafeEnd(s);

                    p.put("update_date", DateUtils.getCurrentTimeStr());
                    list.set(i, p);

                    s = stat.hbaseStat().tsafeBegin();
                    if (analyzer.isMainPost(p))
                        postWriter.batchWrite(p);
                    else
                        cmtWriter.batchWrite(p);
                    stat.hbaseStat().tsafeEnd(s);
                } catch (Throwable e) {
                    LOG.error(e.getMessage(), e);
                } finally {
                }
            }
            docSize.addAndGet(list.size());
        } catch (Exception e) {
            LOG.error("[PROCESS] " + e.getMessage() + " , " + _p);
        }
    }

    @Override
    public void cleanup() {
        if (processSize.get() > 0) {
            try {
                cmtWriter.flush();
            } catch (Throwable e) {
                LOG.error(e.getMessage(), e);
            }

            try {
                postWriter.flush();
            } catch (Throwable e) {
                LOG.error(e.getMessage(), e);
            }
            LOG.info("batch doc size: " + docSize);
        } else {
            ESWriterAPI esWriterAPI = cmtWriter.getEsWriter();
            if (esWriterAPI instanceof ESWriter) {
                ESWriter esWriter = (ESWriter) esWriterAPI;
                esWriter.closeIfIdle();
            }

            esWriterAPI = postWriter.getEsWriter();
            if (esWriterAPI instanceof ESWriter) {
                ESWriter esWriter = (ESWriter) esWriterAPI;
                esWriter.closeIfIdle();
            }
        }
    }
}
