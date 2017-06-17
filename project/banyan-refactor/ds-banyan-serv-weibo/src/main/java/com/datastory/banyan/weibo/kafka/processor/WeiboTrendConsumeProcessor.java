package com.datastory.banyan.weibo.kafka.processor;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.batch.CountUpLatchBlockProcessor;
import com.datastory.banyan.hbase.BanyanRFieldPutter;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.monitor.stat.ETLStatWrapper;
import com.datastory.banyan.utils.CountUpLatch;
import com.datastory.banyan.weibo.doc.RhinoTrendDocMapper;
import com.yeezhao.commons.util.Entity.Params;

import java.io.IOException;

/**
 * com.datastory.banyan.weibo.kafka.processor.WeiboCmtConsumeProcessor
 *
 * @author lhfcws
 * @since 2017/4/27
 */
public class WeiboTrendConsumeProcessor extends CountUpLatchBlockProcessor {

    protected BanyanRFieldPutter writer = new BanyanRFieldPutter(Tables.table(Tables.PH_TREND_TBL));
    protected ETLStatWrapper stat = new ETLStatWrapper();


    public WeiboTrendConsumeProcessor(CountUpLatch latch) {
        super(latch);
    }

    @Override
    public void _process(Object _p) {
        try {
            JSONObject jsonObject = (JSONObject) _p;
            Params trend = new RhinoTrendDocMapper(jsonObject).map();
            if (trend == null) {
                stat.filterStat().inc();
                return;
            }

            // write to hbase
            HBaseUtils.writeTrend(writer, trend);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void cleanup() {
        if (getProcessedSize() > 0) {
            try {
                writer.flush();
            } catch (IOException e) {
                LOG.error(e.getMessage(), e);
            }
            LOG.info("batch size: " + getProcessedSize() + ", stat : " + stat.filterStat());
        }
    }
}
