package com.datastory.banyan.newsforum.kafka.processor;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.batch.CountUpLatchBlockProcessor;
import com.datastory.banyan.hbase.HBaseUtils;
import com.datastory.banyan.hbase.RFieldPutter;
import com.datastory.banyan.newsforum.doc.NFTrendDocMapper;
import com.datastory.banyan.newsforum.doc.Rhino2NewsForumDocMapper;
import com.datastory.banyan.utils.CountUpLatch;
import com.yeezhao.commons.util.CollectionUtil;
import com.yeezhao.commons.util.Entity.Params;
import com.yeezhao.commons.util.serialize.FastJsonSerializer;

import java.io.IOException;
import java.util.List;

/**
 * com.datastory.banyan.newsforum.kafka.processor.NFTrendProcessor
 *
 * @author lhfcws
 * @since 2017/7/5
 */
public class NFTrendProcessor extends CountUpLatchBlockProcessor {
    RFieldPutter putter = new RFieldPutter(Tables.table(Tables.PH_TREND_TBL));

    public NFTrendProcessor(CountUpLatch latch) {
        super(latch);
    }

    @Override
    public void _process(Object _p) {
        try {
            JSONObject jsonObject = (JSONObject) _p;

            List<Params> list = new Rhino2NewsForumDocMapper(jsonObject).map();

            if (CollectionUtil.isEmpty(list)) return;

            for (Params doc : list) {
                Params trendDoc = new NFTrendDocMapper(doc).map();
                LOG.info("[TREND] " + FastJsonSerializer.serialize(trendDoc));
                HBaseUtils.writeTrend(putter, trendDoc);
            }
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void cleanup() {
        try {
            putter.flush();
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
        }
    }
}
