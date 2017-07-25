package com.datastory.banyan.wechat.kafka.processor;

import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.hbase.HBaseReader;
import com.datastory.banyan.kafka.ToESConsumerProcessor;
import com.datastory.banyan.wechat.doc.WxCntHb2ESDocMapper;
import com.datastory.banyan.wechat.es.WxCntESWriter;
import com.datastory.banyan.wechat.hbase.WxContentHBaseReader;
import com.yeezhao.commons.util.Entity.Params;

/**
 * com.datastory.banyan.wechat.kafka.processor.WxContentToESProcessor
 *
 * @author lhfcws
 * @since 16/12/6
 */
@Deprecated
public class WxContentToESProcessor extends ToESConsumerProcessor {
    @Override
    public HBaseReader getHBaseReader() {
        return WxContentHBaseReader.getInstance();
    }

    @Override
    public Params mapDoc(Params p) {
        return new WxCntHb2ESDocMapper(p).map();
    }

    @Override
    public ESWriter getESWriter() {
        return WxCntESWriter.getInstance();
    }
}
