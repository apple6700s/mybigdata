package com.datastory.banyan.weibo.kafka.processor;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.batch.CountUpLatchBlockProcessor;
import com.datastory.banyan.monitor.stat.ETLStatWrapper;
import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.utils.ErrorUtil;
import com.datastory.banyan.weibo.analyz.WbCommentAnalyzer;
import com.datastory.banyan.weibo.doc.Rhino2CmtParamsDocMapper;
import com.datastory.banyan.weibo.doc.RhinoCmtEsDocMapper;
import com.datastory.banyan.weibo.doc.RhinoCmtWeiboEsDocMapper;
import com.datastory.banyan.weibo.es.RhinoCmtESWriter;
import com.datastory.banyan.weibo.es.RhinoCmtWbESWriter;
import com.datastory.banyan.weibo.hbase.PhoenixWbCommentWriter;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.serialize.FastJsonSerializer;

/**
 * com.datastory.banyan.weibo.kafka.processor.WeiboCmtConsumeProcessor
 *
 * @author lhfcws
 * @since 2017/4/27
 */
public class WeiboCmtConsumeProcessor extends CountUpLatchBlockProcessor {

    protected WbCommentAnalyzer analyzer = WbCommentAnalyzer.getInstance();
    protected ETLStatWrapper stat = new ETLStatWrapper();

    protected boolean complex = true;

    public WeiboCmtConsumeProcessor(CountUpLatch latch) {
        super(latch);
    }

    public WeiboCmtConsumeProcessor(CountUpLatch latch, boolean complex) {
        super(latch);
        this.complex = complex;
    }

    @Override
    public void _process(Object _p) {
        try {
            JSONObject jsonObject = (JSONObject) _p;
            Params cmt = new Rhino2CmtParamsDocMapper(jsonObject).map();
            if (cmt == null) {
                stat.filterStat().inc();
                return;
            } else {
                ErrorUtil.infoConsumingProgress(LOG, "WeiboCmt", jsonObject, null);
            }

            cmt = analyzer.analyz(cmt);

            // write to hbase
            PhoenixWbCommentWriter.getInstance().batchWrite(cmt);

            if (complex) {
                // write to es comment
                Params esCmt = new RhinoCmtEsDocMapper(cmt).fillUser().map();
                if (esCmt == null || !esCmt.containsKey("id") || !esCmt.containsKey("_parent")) {
                    LOG.error("[EMPTY esCmt or routing] " + FastJsonSerializer.serialize(cmt + "\n\t =>  " + esCmt));
                } else {
                    RhinoCmtESWriter.getInstance().write(esCmt);
                }

                // write to es weibo
                String weiboMid = cmt.getString("mid");
                RhinoCmtWeiboEsDocMapper rhinoCmtWeiboEsDocMapper = RhinoCmtWeiboEsDocMapper.build(weiboMid);
                Params esWeibo = rhinoCmtWeiboEsDocMapper.fillUser().map();
                if (esWeibo == null || !esWeibo.containsKey("id")) {
                    LOG.error("[EMPTY esWeibo or routing] " + FastJsonSerializer.serialize(cmt + "\n\t =>  " + esWeibo));
                } else {
                    RhinoCmtWbESWriter.getInstance().write(esWeibo);
                }
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void cleanup() {
        if (getProcessedSize() > 0) {
            PhoenixWbCommentWriter.getInstance().flush();
            if (complex) {
                RhinoCmtESWriter.getInstance().flush();
                RhinoCmtWbESWriter.getInstance().flush();
            }
            LOG.info("[WeiboCmt] batch size: " + getProcessedSize() + ", stat : " + stat.filterStat());
        }
    }
}
