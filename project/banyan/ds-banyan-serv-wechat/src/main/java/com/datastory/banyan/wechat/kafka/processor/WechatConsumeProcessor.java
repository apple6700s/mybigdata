package com.datastory.banyan.wechat.kafka.processor;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.batch.CountUpLatchBlockProcessor;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.monitor.mon.RedisMetricMonitor;
import com.datastory.banyan.monitor.stat.AccuStat;
import com.datastory.banyan.monitor.stat.ETLStatWrapper;
import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.wechat.analyz.WechatEssayAnalyzer;
import com.datastory.banyan.wechat.doc.RhinoWechatContentDocMapper;
import com.datastory.banyan.wechat.doc.RhinoWechatMPDocMapper;
import com.datastory.banyan.wechat.hbase.PhoenixWxCntWriter;
import com.datastory.banyan.wechat.hbase.WxMpHBaseWriter;
import com.yeezhao.commons.util.Entity.Params;

/**
 * com.datastory.banyan.wechat.kafka.processor.WechatConsumeProcessor
 *
 * @author lhfcws
 * @since 2017/5/9
 */
public class WechatConsumeProcessor extends CountUpLatchBlockProcessor {
    private PhoenixWxCntWriter cntWriter;
    private WxMpHBaseWriter mpWriter;
    private ETLStatWrapper stat;
    private WechatEssayAnalyzer analyzer;

    public WechatConsumeProcessor(CountUpLatch latch) {
        super(latch);
        cntWriter = PhoenixWxCntWriter.getInstance(2000);
        mpWriter = WxMpHBaseWriter.getInstance(1000);
        stat = new ETLStatWrapper();
        analyzer = WechatEssayAnalyzer.getInstance(false);
    }

    @Override
    public void _process(Object _p) {
        JSONObject jsonObject = null;
        try {
            jsonObject = (JSONObject) _p;
            Params wechat = new RhinoWechatContentDocMapper(jsonObject).map();
            if (wechat == null || wechat.isEmpty()) {
                LOG.error("[FILTER] " + _p);
                stat.filterStat().inc();
                return;
            } else {
                LOG.info("【WECHAT】 udate: " + wechat.getString("update_date") +
                        ", pdate: " + wechat.getString("publish_date") +
                        ", pk: " + wechat.getString("pk") + ", mid: " + wechat.getString("mid")
                );
                AccuStat as = stat.analyzStat().tsafeBegin();
                wechat = analyzer.analyz(wechat);
                stat.analyzStat().tsafeEnd(as);

                as = stat.hbaseStat().tsafeBegin();
                cntWriter.batchWrite(wechat);
                stat.hbaseStat().tsafeEnd(as);
            }

            Params mp = new RhinoWechatMPDocMapper(jsonObject).map();
            if (mp != null) {
                LOG.info("【MP】 udate: " + wechat.getString("update_date") +
                        ", pk: " + mp.getString("pk")
                );
                AccuStat as1 = stat.hbaseStat().tsafeBegin();
                mpWriter.batchWrite(mp);
                stat.hbaseStat().tsafeEnd(as1);
            }
        } catch (Throwable e) {
            stat.errStat().inc();
            String msg;
            if (jsonObject == null)
                msg = "JSON " + _p;
            else
                msg = jsonObject.toJSONString();
            LOG.error("[PROCESS] " + e.getMessage() + " : " + msg);
        } 
    }

    @Override
    public void cleanup() {
        AccuStat as = stat.hbaseStat().tsafeBegin();
        if (processSize.get() > 0) {
            try {
                cntWriter.flush();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }
            try {
                mpWriter.flush();
            } catch (Exception e) {
                LOG.error(e.getMessage(), e);
            }

            LOG.info("Batch size => " + processSize.get() + ", filter content => " + stat.filterStat().getValue() + ", err => " + stat.errStat().getValue());
            stat.hbaseStat().tsafeEnd(as);
            LOG.info(stat.toString());

            if (stat.filterStat().getValue() > 0)
                RedisMetricMonitor.getInstance().inc("filter." + Tables.table(Tables.PH_WXCNT_TBL), stat.filterStat().getValue());
        } else {
            ESWriterAPI esWriterAPI = cntWriter.getEsWriter();
            if (esWriterAPI != null && esWriterAPI instanceof ESWriter) {
                ESWriter esWriter = (ESWriter) esWriterAPI;
                esWriter.closeIfIdle();
            }

            esWriterAPI = mpWriter.getEsWriter();
            if (esWriterAPI != null && esWriterAPI instanceof ESWriter) {
                ESWriter esWriter = (ESWriter) esWriterAPI;
                esWriter.closeIfIdle();
            }
        }
    }
}
