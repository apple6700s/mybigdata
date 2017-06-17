package com.datastory.banyan.weibo.kafka.processor;

import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.hbase.HBaseReader;
import com.datastory.banyan.kafka.ToESConsumerProcessor;
import com.datastory.banyan.weibo.doc.WbCnt2RhinoESDocMapper;
import com.datastory.banyan.weibo.es.WbCntESWriter;
import com.datastory.banyan.weibo.hbase.WbCntHBaseReader;
import com.yeezhao.commons.util.Entity.Params;

/**
 * com.datastory.banyan.weibo.kafka.processor.WeiboContentToESProcessor
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class WeiboContentToESProcessor extends ToESConsumerProcessor {
    @Override
    public HBaseReader getHBaseReader() {
        return WbCntHBaseReader.getInstance();
    }

    @Override
    public Params mapDoc(Params p) {
//        return new WbCntHb2ESDocMapper(p).map();
        return new WbCnt2RhinoESDocMapper(p).map();
    }

    @Override
    public ESWriter getESWriter() {
        return WbCntESWriter.getInstance();
    }
}
