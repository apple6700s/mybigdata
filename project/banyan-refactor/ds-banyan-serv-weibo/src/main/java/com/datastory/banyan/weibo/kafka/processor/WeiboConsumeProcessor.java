package com.datastory.banyan.weibo.kafka.processor;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.batch.CountUpLatchBlockProcessor;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.monitor.mon.RedisMetricMonitor;
import com.datastory.banyan.monitor.stat.ETLStatWrapper;
import com.datastory.banyan.utils.BanyanTypeUtil;
import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.utils.DateUtils;
import com.datastory.banyan.weibo.analyz.WbContentAnalyzer;
import com.datastory.banyan.weibo.analyz.WbUserAnalyzer;
import com.datastory.banyan.weibo.doc.Status2HbParamsDocMapper;
import com.datastory.banyan.weibo.doc.User2HbParamsDocMapper;
import com.datastory.banyan.weibo.hbase.PhoenixWbContentWriter;
import com.datastory.banyan.weibo.hbase.WbUserHBaseWriter;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.serialize.GsonSerializer;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import weibo4j.model.Status;
import weibo4j.model.User;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * com.datastory.banyan.weibo.kafka.processor.WeiboConsumeProcessor
 *
 * @author lhfcws
 * @since 2017/4/26
 */
public class WeiboConsumeProcessor extends CountUpLatchBlockProcessor {
    protected static Logger LOG = Logger.getLogger(WeiboConsumeProcessor.class);

    protected AtomicInteger statusSize = new AtomicInteger(0);
    protected AtomicInteger userSize = new AtomicInteger(0);
    protected PhoenixWbContentWriter cntWriter = PhoenixWbContentWriter.getInstance(3000);
    protected WbUserHBaseWriter userWriter = WbUserHBaseWriter.getInstance(1000);
    protected WbContentAnalyzer cntAnalyzer = WbContentAnalyzer.getInstance();
    protected WbUserAnalyzer userAnalyzer = WbUserAnalyzer.getInstance();
    protected ETLStatWrapper stat = new ETLStatWrapper();

    public WeiboConsumeProcessor(CountUpLatch latch) {
        super(latch);
    }

    @Override
    public void _process(Object _p) {
        int filterCnt = 0;
        try {
            JSONObject jsonObject = (JSONObject) _p;

            Status status;
            try {
                status = GsonSerializer.deserialize(jsonObject.getString("json"), Status.class);
            } catch (Exception e) {
                LOG.error(e.getMessage());
                filterCnt++;
                return;
            }

            if (status.getUser() == null || StringUtils.isEmpty(status.getUser().getId())) {
                filterCnt++;
                return;
            }
            String updateDate = jsonObject.getString("update_date");

            // split status & user
            User statusUser = status.getUser();
            // 当前微博系统显示转发的那一条就是源微博，用//等可以分隔出大致的转发关系，也就是说转发链被压缩到用户发的微博中。
            Status srcStatus = status.getRetweetedStatus();
            User srcStatusUser = srcStatus == null ? null : srcStatus.getUser();
            LOG.info("udate: " + updateDate + ", pdate: " +
                    DateUtils.getTimeStr(status.getCreatedAt())
            );

            // doc mapping status & user
            Params user = new User2HbParamsDocMapper(statusUser).map();
            Params weibo = new Status2HbParamsDocMapper(status).map();
            Params srcWeibo = new Status2HbParamsDocMapper(srcStatus).map();
            Params srcUser = new User2HbParamsDocMapper(srcStatusUser).map();

            if (weibo != null && user != null) {
                user.put("last_tweet_date", weibo.getString("publish_date"));
                BanyanTypeUtil.safePut(weibo, "uid", user.getString("uid"));
                BanyanTypeUtil.safePut(weibo, "username", user.getString("name"));
            }

            if (srcWeibo != null && srcUser != null) {
                srcUser.put("last_tweet_date", srcWeibo.getString("publish_date"));
                if (user.get("uid") != null)
                    srcWeibo.put("uid", srcUser.getString("uid"));
                weibo.put("src_content", srcWeibo.getString("content"));
                weibo.put("src_mid", srcWeibo.getString("mid"));
                weibo.put("src_uid", srcUser.getString("uid"));
                BanyanTypeUtil.safePut(srcWeibo, "uid", srcUser.getString("uid"));
                BanyanTypeUtil.safePut(srcWeibo, "username", srcUser.getString("name"));
            }

            // analyz
            weibo = cntAnalyzer.analyz(weibo);
            srcWeibo = cntAnalyzer.analyz(srcWeibo);
            user = userAnalyzer.analyz(user);
            srcUser = userAnalyzer.analyz(srcUser);

            // write to hbase
            try {
                if (weibo != null) {
                    cntWriter.batchWrite(weibo);
                    statusSize.incrementAndGet();
                } else
                    filterCnt++;
            } catch (Throwable e) {
                LOG.error(e.getMessage(), e);
            }

            try {
                if (srcWeibo != null) {
                    cntWriter.batchWrite(srcWeibo);
                    statusSize.incrementAndGet();
                }
            } catch (Throwable e) {
                LOG.error(e.getMessage(), e);
            }

            try {
                if (user != null) {
                    userWriter.batchWrite(user);
                    userSize.incrementAndGet();
                }
            } catch (Throwable e) {
                LOG.error(e.getMessage(), e);
            }

            try {
                if (srcUser != null) {
                    userWriter.batchWrite(srcUser);
                    userSize.incrementAndGet();
                }
            } catch (Throwable e) {
                LOG.error(e.getMessage(), e);
            }
        } catch (Throwable e) {
            LOG.error("[PROCESS] " + e.getMessage() + " , " + _p);
        } finally {
            if (latch != null)
                latch.countup();
            if (filterCnt > 0) {
                LOG.error("[FILTER] " + _p);
                stat.filterStat().inc();
            }
        }
    }

    @Override
    public void cleanup() {
        long size = processSize.get();


        if (size > 0) {
            try {
                cntWriter.flush();
            } catch (Throwable e) {
                LOG.error(e.getMessage(), e);
            }
            try {
                userWriter.flush();
            } catch (Throwable e) {
                LOG.error(e.getMessage(), e);
            }
            LOG.info("batch size: " + size + ", statusSize: " + statusSize.get() + ", userSize: " + userSize.get());
            if (stat.filterStat().getValue() > 0)
                RedisMetricMonitor.getInstance().inc("filter." + Tables.table(Tables.PH_WBCNT_TBL), stat.filterStat().getValue());
        } else {
            ESWriterAPI esWriterAPI = cntWriter.getEsWriter();
            if (esWriterAPI instanceof ESWriter) {
                ESWriter esWriter = (ESWriter) esWriterAPI;
                esWriter.closeIfIdle();
            }
        }

        // set null
        stat = null;
        cntAnalyzer = null;
        cntWriter = null;
        userAnalyzer = null;
        userWriter = null;
        userSize = null;
        statusSize = null;
    }
}
