package com.datastory.banyan.asyncdata.video.kafka;

import com.alibaba.fastjson.JSONObject;
import com.datastory.banyan.asyncdata.video.doc.RhinoVideoCmtDocMapper;
import com.datastory.banyan.asyncdata.video.hbase.VdCommentPhoenixWriter;
import com.datastory.banyan.batch.CountUpLatchBlockProcessor;
import com.datastory.banyan.es.ESWriter;
import com.datastory.banyan.es.ESWriterAPI;
import com.datastory.banyan.hbase.PhoenixWriter;
import com.datastory.banyan.utils.CountUpLatch;
import com.yeezhao.commons.util.Entity.Params;

/**
 * com.datastory.banyan.asyncdata.video.kafka.VideoPostProcessor
 *
 * @author lhfcws
 * @since 2017/4/12
 */
public class VideoCmtProcessor extends CountUpLatchBlockProcessor {
    private PhoenixWriter writer;

    public VideoCmtProcessor(CountUpLatch latch) {
        super(latch);
        writer = VdCommentPhoenixWriter.getInstance();
    }

    @Override
    public void _process(Object _p) {
        try {
            JSONObject jsonObject = (JSONObject) _p;
            Params hbParams = new RhinoVideoCmtDocMapper(jsonObject).map();
            writer.batchWrite(hbParams);
        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
        }
    }

    @Override
    public void cleanup() {
        if (processSize.get() > 0) {
            writer.flush();
        } else {
            ESWriterAPI esWriterAPI = writer.getEsWriter();
            if (esWriterAPI instanceof ESWriter) {
                ESWriter esWriter = (ESWriter) esWriterAPI;
                esWriter.closeIfIdle();
            }
        }
    }
}
