package com.datastory.banyan.asyncdata.video.hbase;

import com.datastory.banyan.asyncdata.video.doc.Hb2EsVdCommentDocMapper;
import com.datastory.banyan.asyncdata.video.es.VdCmtEsWriter;
import com.datastory.banyan.base.Tables;
import com.datastory.banyan.hbase.PhoenixWriter;

/**
 * com.datastory.banyan.asyncdata.video.hbase.VdPostPhoenixWriter
 *
 * @author lhfcws
 * @since 2017/4/10
 */
public class VdCommentPhoenixWriter extends PhoenixWriter {
    private static volatile VdCommentPhoenixWriter _singleton = null;

    public static VdCommentPhoenixWriter getInstance() {
        if (_singleton == null) {
            synchronized (VdCommentPhoenixWriter.class) {
                if (_singleton == null) {
                    _singleton = new VdCommentPhoenixWriter();
                }
            }
        }
        return _singleton;
    }

    public VdCommentPhoenixWriter() {
        setEsWriterHook(VdCmtEsWriter.getInstance(), Hb2EsVdCommentDocMapper.class);
    }

    @Override
    public String getTable() {
        return Tables.table(Tables.PH_VIDEO_CMT_TBL);
    }
}
