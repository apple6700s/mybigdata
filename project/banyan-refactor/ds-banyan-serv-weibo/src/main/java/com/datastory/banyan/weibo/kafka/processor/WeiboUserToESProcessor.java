package com.datastory.banyan.weibo.kafka.processor;

import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.hbase.HBaseReader;
import com.datastory.banyan.kafka.ToESConsumerProcessor;
import com.datastory.banyan.weibo.doc.WbUser2RhinoESDocMapper;
import com.datastory.banyan.weibo.doc.WbUserHb2ESDocMapper;
import com.datastory.banyan.weibo.es.WbUserESWriter;
import com.datastory.banyan.weibo.hbase.WbUserHBaseReader;
import com.yeezhao.commons.util.Entity.Params;

/**
 * com.datastory.banyan.weibo.kafka.processor.WeiboUserToESProcessor
 *
 * @author lhfcws
 * @since 16/11/24
 */

public class WeiboUserToESProcessor extends ToESConsumerProcessor {
    @Override
    public HBaseReader getHBaseReader() {
        return WbUserHBaseReader.getInstance();
    }

    @Override
    public Params mapDoc(Params p) {
//        return new WbUserHb2ESDocMapper(p).map();
        return new WbUser2RhinoESDocMapper(p).map();
    }

    @Override
    public ESWriter getESWriter() {
        return WbUserESWriter.getInstance();
    }
}
