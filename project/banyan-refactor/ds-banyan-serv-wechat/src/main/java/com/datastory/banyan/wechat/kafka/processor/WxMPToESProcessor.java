package com.datastory.banyan.wechat.kafka.processor;

import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.hbase.HBaseReader;
import com.datastory.banyan.kafka.ToESConsumerProcessor;
import com.datastory.banyan.wechat.doc.WxMPHb2ESDocMapper;
import com.datastory.banyan.wechat.es.WxMPESWriter;
import com.datastory.banyan.wechat.hbase.WxMPHBaseReader;
import com.yeezhao.commons.util.Entity.Params;
import org.elasticsearch.action.index.IndexRequest;

import java.io.IOException;
import java.util.List;

/**
 * com.datastory.banyan.wechat.kafka.processor.WxMPToESProcessor
 *
 * @author lhfcws
 * @since 16/12/12
 */

public class WxMPToESProcessor extends ToESConsumerProcessor {
    @Override
    public HBaseReader getHBaseReader() {
        return WxMPHBaseReader.getInstance();
    }

    @Override
    public Params mapDoc(Params p) {
        return new WxMPHb2ESDocMapper(p).map();
    }

    @Override
    public ESWriter getESWriter() {
        return WxMPESWriter.getInstance();
    }

//    @Override
//    protected void writeEs(List<Params> reads) {
//        ESWriter esWriter = getESWriter();
//        for (Params p : reads) {
//            if (p == null || p.size() <= 1)
//                continue;
//            Params esDoc = mapDoc(p);
//            try {
//                esWriter.write(esDoc);
//            } catch (Exception e) {
//                LOG.error(e.getMessage(), e);
//            }
//        }
////        if (reads.size() > 0)
////            esWriter.flush();
//    }
}
